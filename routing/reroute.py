import os
import time
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
from shapely.geometry import Point, LineString
from shapely import wkt
from pyproj import Transformer

from db.db_connection import (
    load_from_db,
    fetch_active_clients,
    fetch_latest_location,
    has_departure_candidate, save_reroute,
)
from selector import evaluate_and_store_best_route

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("reroute")

# --- Tunables ---
TICK_SECONDS = 5  # how often to scan active clients
DEVIATION_METERS_DIRECT = 35.0  # off-path threshold for A* direct routes
DEVIATION_METERS_MAPF = 60.0  # off-path threshold for MAPF legs
DEVIATION_STREAKS_REQUIRED = 2  # must fail this many ticks consecutively
DELAY_THRESHOLD_SECONDS = 180  # if GTFS-RT delay grows beyond this, reroute
DEPARTURE_STALE_SECONDS = 45  # if departure already passed this much, reroute

# metric projection (fast + fine for city scale)
_transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

# in-memory debounce per client (stateless across restarts; safe)
_off_counts = {}  # client_id -> consecutive_off_path_count


def _meters_point_to_linestring(lat, lon, line_wkt):
    """Compute point-to-line closest distance in meters (projecting to EPSG:3857)."""
    if not line_wkt:
        return float("inf")
    try:
        line = wkt.loads(line_wkt)
        if not isinstance(line, LineString) or line.is_empty:
            return float("inf")
    except Exception:
        return float("inf")

    x, y = _transformer.transform(lon, lat)
    px = Point(x, y)
    # project full line to metric once for speed
    xs, ys = zip(*[_transformer.transform(pt[0], pt[1]) for pt in line.coords])
    line_m = LineString(list(zip(xs, ys)))
    return px.distance(line_m)  # meters in EPSG:3857


def _fetch_current_choice(client_id):
    """
    Get latest chosen route for client from optimized_routes.
    Returns dict or None:
      { segment_type, stop_id, created_at (UTC), path_wkt }
    """
    q = """
        SELECT segment_type, stop_id, ST_AsText(path) AS path, created_at
        FROM optimized_routes
        WHERE client_id = %s AND is_chosen = TRUE
        ORDER BY created_at DESC
        LIMIT 1;
    """
    rows = load_from_db(q, (client_id,))
    if not rows:
        return None
    r = rows[0]
    return {
        "segment_type": (r.get("segment_type") or "").lower(),
        "stop_id": r.get("stop_id"),
        "path_wkt": r.get("path"),
        "created_at": r.get("created_at"),
    }


def _latest_departure_snapshot(client_id, stop_id):
    """
    Peek at the current best departure for this stop from view_departure_candidates.
    Returns dict or None.
    """
    q = """
        SELECT departure_time, delay_seconds, status, route_id, trip_id
        FROM view_departure_candidates
        WHERE client_id = %s AND stop_id = %s
        ORDER BY COALESCE(delay_seconds, 0) ASC, departure_time ASC
        LIMIT 1;
    """
    rows = load_from_db(q, (client_id, stop_id))
    return rows[0] if rows else None


def _needs_reroute_for_deviation(client_id, choice, lat, lon):
    """
    Check geometric deviation from advised path.
    Returns (bool, reason_str)
    """
    if not choice or not choice["path_wkt"]:
        return True, "no_path_in_choice"

    dist = _meters_point_to_linestring(lat, lon, choice["path_wkt"])
    thr = DEVIATION_METERS_MAPF if choice["segment_type"] == "multimodal" else DEVIATION_METERS_DIRECT

    off = dist > thr
    if off:
        _off_counts[client_id] = _off_counts.get(client_id, 0) + 1
    else:
        _off_counts[client_id] = 0

    if _off_counts[client_id] >= DEVIATION_STREAKS_REQUIRED:
        return True, f"off_path_{int(dist)}m"

    return False, ""


def _needs_reroute_for_gtfs(client_id, choice):
    """
    For MAPF choices, verify there’s still a viable departure aligned with ETA.
    Returns (bool, reason_str)
    """
    if not choice or choice["segment_type"] != "multimodal":
        return False, ""

    stop_id = choice["stop_id"]
    if not stop_id:
        return True, "missing_stop_id"

    # Quick availability check (fast path)
    if not has_departure_candidate(client_id, stop_id):
        return True, "no_departure_candidate"

    # Deeper check: delay or passed departure
    dep = _latest_departure_snapshot(client_id, stop_id)
    if not dep:
        return True, "no_departure_row"

    dep_time = dep.get("departure_time")
    delay = float(dep.get("delay_seconds") or 0.0)
    now = datetime.now(timezone.utc)

    if isinstance(dep_time, str):
        # some drivers might return string; let pandas parse
        dep_time = pd.to_datetime(dep_time, utc=True).to_pydatetime()

    if dep_time and (now - dep_time) > timedelta(seconds=DEPARTURE_STALE_SECONDS):
        return True, "departure_passed"

    if delay > DELAY_THRESHOLD_SECONDS:
        return True, f"delay_{int(delay)}s"

    return False, ""


def _fetch_current_choice_raw(client_id):
    q = """
      SELECT stop_id, segment_type, origin_lat, origin_lon,
             destination_lat, destination_lon, ST_AsText(path) AS path
      FROM view_routes_live
      WHERE client_id = %s
      LIMIT 1;
    """
    rows = load_from_db(q, (client_id,))
    return rows[0] if rows else None


def _reroute_client(client_id, reason):
    before = _fetch_current_choice_raw(client_id)  # snapshot
    logging.info(f"🔁 Rerouting {client_id} due to {reason}")
    evaluate_and_store_best_route(client_id)  # computes & writes to optimized_routes

    after = _fetch_current_choice_raw(client_id)
    if not after:
        return

    # If changed (segment/stop or path), persist reroute event
    changed = (not before or
               (before.get("segment_type") or "") != (after.get("segment_type") or "") or
               (before.get("stop_id") or "") != (after.get("stop_id") or "") or
               (before.get("path") or "") != (after.get("path") or ""))

    if changed:
        save_reroute(
            client_id=client_id,
            stop_id=after.get("stop_id"),
            destination_coords=(after["destination_lat"], after["destination_lon"]),
            path_wkt=(after.get("path") or "LINESTRING EMPTY"),
            segment_type=(after.get("segment_type") or "unknown"),
            reason=reason,
            origin_coords=(after.get("origin_lat"), after.get("origin_lon")),
            previous_stop_id=(before.get("stop_id") if before else None),
            previous_segment_type=(before.get("segment_type") if before else None),
        )


def loop_once():
    clients = fetch_active_clients()
    if not clients:
        return

    for client_id in clients:
        choice = _fetch_current_choice(client_id)

        # latest location (we already debounce in deviation check)
        loc = fetch_latest_location(client_id)
        if not loc:
            continue
        lat, lon, _ = loc

        # 1) Deviation?
        need_dev, why_dev = _needs_reroute_for_deviation(client_id, choice, lat, lon)
        if need_dev:
            _reroute_client(client_id, why_dev)
            continue  # after reroute we’ll check again next tick

        # 2) GTFS-RT shift (only for MAPF)
        need_gtfs, why_gtfs = _needs_reroute_for_gtfs(client_id, choice)
        if need_gtfs:
            _reroute_client(client_id, why_gtfs)
            continue


def main():
    log.info("🧭 reroute loop started.")
    while True:
        try:
            loop_once()
        except Exception as e:
            log.error(f"Reroute loop error: {e}")
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    main()
