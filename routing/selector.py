import logging
from datetime import datetime
import numpy as np
from shapely.geometry import LineString   # ✅ fix: was `from shapely import LineString`
from shapely.wkt import loads

from db.db_connection import (
    load_from_db,
    fetch_best_combined_poi,
    fetch_latest_location,
    save_optimized_route,
    save_to_db,
    get_latest_speed,
    get_route_usage_ratios,  # returns (astar_ratio, mapf_ratio)
    load_lstm_model,
)

logging.basicConfig(level=logging.INFO)

# --- Tunables, small & conservative ---
HISTORY_BLEND = 0.15           # how much to blend in (astar_ratio,mapf_ratio)
KNOWN_LINE_NUDGE = 0.05        # extra bump if MAPF dep is on a “favorite” route
CLOSE_MARGIN = 0.10            # if scores within this, treat as “too close to call”
MAPF_PENALTY_METERS = 100.0    # heuristic fallback penalty for MAPF distance


def _build_candidate_features(speed, hour_norm, ratios, dist, is_multimodal):
    """
    Fixed-size feature vector per candidate:
      [dist_norm, is_multimodal, hour_norm, speed_norm, astar_ratio, mapf_ratio]
    """
    # normalization guardrails (tune as needed)
    dist_norm = max(0.0, min(float(dist) / 5000.0, 1.0))  # ~5km scale
    speed_norm = max(0.0, min(float(speed) / 6.0, 1.0))   # ~6 m/s cap

    astar_ratio, mapf_ratio = ratios
    return np.array([
        dist_norm,
        1.0 if is_multimodal else 0.0,
        hour_norm,
        speed_norm,
        float(astar_ratio),
        float(mapf_ratio),
    ], dtype=np.float32)


def _predict_with_lstm(astar_feat, mapf_feat):
    """
    Returns: ('astar'|'mapf', [score_astar, score_mapf])
    Raises RuntimeError if model missing or output shape unexpected → caller falls back.
    """
    model = load_lstm_model()
    if model is None:
        raise RuntimeError("LSTM model not available")

    # model expects (batch, timesteps, features)
    seq = np.stack([astar_feat, mapf_feat], axis=0)  # (2, F)
    seq = np.expand_dims(seq, axis=0)               # (1, 2, F)
    preds = model.predict(seq, verbose=0)

    arr = np.array(preds).squeeze()
    if arr.ndim == 1 and arr.shape[0] == 2:
        score_astar, score_mapf = float(arr[0]), float(arr[1])
    elif arr.ndim == 0 or (arr.ndim == 1 and arr.shape[0] == 1):
        s = float(arr if np.isscalar(arr) else arr[0])
        score_mapf = s
        score_astar = 1.0 - s
    else:
        raise RuntimeError(f"Unexpected LSTM output shape: {arr.shape}")

    return ("mapf" if score_mapf > score_astar else "astar", [score_astar, score_mapf])


def _fetch_best_departure_candidate(client_id, stop_id):
    """
    Pick the best departure aligned with predicted_eta:
    - within the ETA window (view logic)
    - lowest delay, earliest departure
    """
    q = """
        SELECT
            trip_id, departure_time, arrival_time, delay_seconds, status,
            route_id, direction_id, trip_headsign
        FROM view_departure_candidates
        WHERE client_id = %s AND stop_id = %s
        ORDER BY COALESCE(delay_seconds, 0) ASC, departure_time ASC
        LIMIT 1;
    """
    rows = load_from_db(q, (client_id, stop_id))
    return rows[0] if rows else None


def _fetch_switch_profile_seconds(client_id, stop_id):
    """
    Return avg_switch_seconds for (client_id, stop_id) if present.
    """
    q = """
        SELECT avg_switch_seconds
        FROM client_switch_profiles
        WHERE client_id = %s AND stop_id = %s
        LIMIT 1;
    """
    rows = load_from_db(q, (client_id, stop_id))
    if rows and "avg_switch_seconds" in rows[0]:
        return rows[0]["avg_switch_seconds"]
    return None


def _get_top_routes(client_id, k=5):
    """
    Client's top-N historical route_ids based on view_departure_candidates history.
    Keeps things simple & aligned with the data you already materialize.
    """
    q = """
        SELECT route_id, COUNT(*) AS cnt
        FROM view_departure_candidates
        WHERE client_id = %s AND route_id IS NOT NULL
        GROUP BY route_id
        ORDER BY cnt DESC
        LIMIT %s;
    """
    rows = load_from_db(q, (client_id, k))
    return {r["route_id"] for r in rows} if rows else set()


def _blend_with_history(scores, ratios, dep_route_id, top_routes):
    """
    Soft-blend model scores with historical usage ratios, then nudge if the MAPF
    departure is on a known/favorite route.
      - scores: [score_astar, score_mapf] from model
      - ratios: (astar_ratio, mapf_ratio) historical usage
    Returns blended [p_astar, p_mapf] (sum ≈ 1).
    """
    # normalize model scores to pseudo-probabilities
    s = np.array(scores, dtype=float)
    # small epsilon so zero vectors don’t explode
    s = s - s.min()  # shift to >= 0
    denom = s.sum()
    if denom <= 1e-9:
        p_model = np.array([0.5, 0.5], dtype=float)
    else:
        p_model = s / denom

    astar_ratio, mapf_ratio = ratios
    p_hist = np.array([float(astar_ratio), float(mapf_ratio)], dtype=float)
    if p_hist.sum() <= 1e-9:
        p_hist = np.array([0.5, 0.5], dtype=float)
    else:
        p_hist = p_hist / p_hist.sum()

    blended = (1.0 - HISTORY_BLEND) * p_model + HISTORY_BLEND * p_hist

    # known-line nudge (subtle) if departure is on a favorite route
    if dep_route_id and dep_route_id in top_routes:
        blended[1] = min(1.0, blended[1] + KNOWN_LINE_NUDGE)

    # re-normalize lightly
    total = blended.sum()
    if total > 1e-9:
        blended = blended / total

    return blended  # [p_astar, p_mapf]


def evaluate_and_store_best_route(client_id):
    # 1) Target POI (detected + predicted + live)
    poi = fetch_best_combined_poi(client_id)
    if not poi:
        logging.warning(f"⚠ No best POI found for {client_id}")
        return

    lat, lon = poi["lat"], poi["lon"]

    # 2) Latest A* route (seed one if missing)
    astar_q = """
        SELECT distance, ST_AsText(path) AS path, poi_id, origin_lat, origin_lon, created_at
        FROM astar_routes
        WHERE client_id = %s
          AND target_type = 'poi'
          AND destination_lat = %s
          AND destination_lon = %s
        ORDER BY created_at DESC
        LIMIT 1;
    """
    astar_rows = load_from_db(astar_q, (client_id, lat, lon))

    if not astar_rows:
        logging.warning(f"⚠ No A* route to POI for {client_id}. Triggering fallback insert.")
        origin = fetch_latest_location(client_id)
        if not origin:
            logging.warning(f"❌ Cannot fallback for {client_id} — missing location.")
            return
        origin_lat, origin_lon, _ = origin

        # seed a minimal astar row so downstream pieces can heal
        fallback_astar_insert = """
            INSERT INTO astar_routes (
                client_id, target_type, poi_id,
                origin_lat, origin_lon,
                destination_lat, destination_lon,
                path, distance,
                efficiency_score, decision_context, predicted_eta
            )
            VALUES (%s, 'poi', NULL,
                    %s, %s, %s, %s,
                    NULL, 0, 0, 'fallback_astar', NOW())
            ON CONFLICT DO NOTHING;
        """
        save_to_db(fallback_astar_insert, (client_id, origin_lat, origin_lon, lat, lon))

        # store a noop/fallback optimized route so planner has something
        save_optimized_route(
            client_id,
            stop_id="direct",
            destination_coords=(lat, lon),
            path=LineString(),
            segment_type="fallback",
            is_chosen=True,
            origin_coords=(origin_lat, origin_lon)
        )
        return

    astar = astar_rows[0]
    try:
        astar_path = loads(astar["path"]) if astar["path"] else LineString()
    except Exception:
        astar_path = LineString()
    astar_dist = float(astar.get("distance") or 0.0)
    origin_lat = astar.get("origin_lat")
    origin_lon = astar.get("origin_lon")

    # 3) Optional MAPF candidate
    mapf_q = """
        SELECT stop_id, distance, ST_AsText(path) AS path, created_at
        FROM mapf_routes
        WHERE client_id = %s
          AND destination_lat = %s
          AND destination_lon = %s
          AND success = TRUE
        ORDER BY created_at DESC
        LIMIT 1;
    """
    mapf_rows = load_from_db(mapf_q, (client_id, lat, lon))

    # If no MAPF available → choose A*
    if not mapf_rows:
        logging.info(f"🛡️ No MAPF route. Using A* for {client_id}")
        save_optimized_route(
            client_id, stop_id="direct",
            destination_coords=(lat, lon),
            path=astar_path,
            segment_type="direct",
            origin_coords=(origin_lat, origin_lon)
        )
        return

    mapf = mapf_rows[0]
    stop_id = mapf["stop_id"]
    mapf_dist = float(mapf.get("distance") or 0.0)
    try:
        mapf_path = loads(mapf["path"]) if mapf["path"] else LineString()
    except Exception:
        mapf_path = LineString()

    # 4) Live departures & preferences context
    dep = _fetch_best_departure_candidate(client_id, stop_id)
    if not dep:
        # No aligned departure → MAPF not viable; fall back to A*
        logging.info(f"🚫 No aligned departure at stop {stop_id}. Falling back to A* for {client_id}")
        save_optimized_route(
            client_id, stop_id="direct",
            destination_coords=(lat, lon),
            path=astar_path,
            segment_type="direct",
            origin_coords=(origin_lat, origin_lon)
        )
        return

    delay = float(dep["delay_seconds"] or 0.0)
    route_id = dep.get("route_id")
    headsign = dep.get("trip_headsign")
    logging.info(f"🚌 Candidate departure for {client_id} at stop {stop_id}: "
                 f"route {route_id}, headsign '{headsign}', delay {delay:.0f}s")

    # 5) LSTM scoring + soft history blend + known-line nudge
    try:
        speed = get_latest_speed(client_id) or 0.0
        hour_norm = datetime.utcnow().hour / 23.0
        ratios = get_route_usage_ratios(client_id)  # (astar_ratio, mapf_ratio)
        astar_feat = _build_candidate_features(speed, hour_norm, ratios, astar_dist, is_multimodal=False)
        mapf_feat  = _build_candidate_features(speed, hour_norm, ratios, mapf_dist,  is_multimodal=True)

        choice_raw, scores = _predict_with_lstm(astar_feat, mapf_feat)
        logging.info(f"🤖 LSTM raw scores {client_id}: A*={scores[0]:.3f}, MAPF={scores[1]:.3f}")

        # blend with history, nudge for favorite lines
        top_routes = _get_top_routes(client_id, k=5)
        blended = _blend_with_history(scores, ratios, route_id, top_routes)  # [p_astar, p_mapf]
        logging.info(f"🧪 Blended probs {client_id}: A*={blended[0]:.3f}, MAPF={blended[1]:.3f} "
                     f"(history {HISTORY_BLEND:.2f}"
                     f"{', +nudge' if route_id in top_routes else ''})")

        # “too close” tie-breaker → lean MAPF when live delay small & client switches fast
        margin = abs(blended[1] - blended[0])
        final_choice = "mapf" if blended[1] > blended[0] else "astar"
        if margin < CLOSE_MARGIN:
            avg_switch = _fetch_switch_profile_seconds(client_id, stop_id)
            if avg_switch is not None and delay <= 60 and avg_switch <= 120:
                logging.info(f"🔧 Tie‑breaker nudges MAPF (delay={delay:.0f}s, avg_switch={avg_switch}s)")
                final_choice = "mapf"

        if final_choice == "astar":
            save_optimized_route(
                client_id, stop_id="direct",
                destination_coords=(lat, lon),
                path=astar_path,
                segment_type="direct",
                origin_coords=(origin_lat, origin_lon)
            )
        else:
            save_optimized_route(
                client_id, stop_id=stop_id,
                destination_coords=(lat, lon),
                path=mapf_path,
                segment_type="multimodal",
                origin_coords=(origin_lat, origin_lon)
            )
        return

    except Exception as e:
        logging.warning(f"⚠ LSTM scoring failed for {client_id}: {e} — falling back to heuristic")

    # 6) Heuristic fallback (distance + fixed MAPF penalty + live delay)
    mapf_total = mapf_dist + MAPF_PENALTY_METERS + max(0.0, delay)
    if astar_dist < mapf_total:
        logging.info(f"✅ Heuristic chose A* for {client_id} — {astar_dist:.1f}m vs MAPF {mapf_total:.1f} (inc. delay)")
        save_optimized_route(
            client_id, stop_id="direct",
            destination_coords=(lat, lon),
            path=astar_path,
            segment_type="direct",
            origin_coords=(origin_lat, origin_lon)
        )
    else:
        logging.info(f"✅ Heuristic chose MAPF for {client_id} — {mapf_total:.1f} vs A* {astar_dist:.1f}m")
        save_optimized_route(
            client_id, stop_id=stop_id,
            destination_coords=(lat, lon),
            path=mapf_path,
            segment_type="multimodal",
            origin_coords=(origin_lat, origin_lon)
        )
