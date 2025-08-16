import logging
from shapely.wkt import loads as load_wkt
from shapely.geometry import LineString
from collections import defaultdict
from db.db_connection import (
    fetch_recent_clients_from_trajectories,
    fetch_predicted_pois_sequence,
    fetch_matching_final_route,
    enrich_stop_meta,
    save_weekly_schedule_entry
)

logging.basicConfig(level=logging.INFO)


def generate_schedule_for_client(client_id, prediction_type):
    pois = fetch_predicted_pois_sequence(client_id, prediction_type)
    if not pois or len(pois) < 2:
        logging.info(f"⚠ Not enough POIs to build a path for {client_id}")
        return

    # Group POIs by weekday of predicted time
    days = defaultdict(list)
    for poi in pois:
        weekday = poi["timestamp"].strftime("%A")
        days[weekday].append(poi)

    for day, sequence in days.items():
        if len(sequence) < 2:
            continue

        segments = []
        seg_types = []
        stop_ids = []

        for i in range(len(sequence) - 1):
            origin = sequence[i]
            dest = sequence[i + 1]

            route = fetch_matching_final_route(
                client_id, dest["lat"], dest["lon"]
            )
            if not route or not route.get("path_text"):
                logging.warning(
                    f"⚠ No route for {client_id}: {origin['lat']},{origin['lon']} → {dest['lat']},{dest['lon']}"
                )
                continue

            try:
                path = load_wkt(route["path_text"])
                if path.is_empty or len(path.coords) < 2:
                    continue
                segments.append(path)
                seg_types.append(route.get("segment_type") or "unknown")
                stop_ids.append(route.get("stop_id"))
            except Exception as e:
                logging.error(f"❌ WKT parse failed for {client_id}: {e}")

        if not segments:
            logging.info(f"⚠ No valid segments built for {client_id} on {day}")
            continue

        # Concatenate segments
        full_path = LineString([pt for seg in segments for pt in seg.coords])

        # Anchor time: the first “arrival” of the day
        anchor_time = sequence[1]["timestamp"]

        # Optional: enrich with stop meta from the last segment’s stop_id
        stop_meta = enrich_stop_meta(stop_ids[-1]) if stop_ids and stop_ids[-1] else None

        save_weekly_schedule_entry(
            client_id=client_id,
            visit_day=day,
            predicted_time=anchor_time,
            lat=sequence[0]["lat"],
            lon=sequence[0]["lon"],
            path_wkt=full_path.wkt,
            segment_type="behavioral_path",
            prediction_type=prediction_type,
        )
        logging.info(
            f"✅ Saved schedule for {client_id} on {day} with {len(segments)} segments "
            f"(mix={set(seg_types)})"
        )


def run_weekly_planner(prediction_type="weekly"):
    logging.info(f"📆 Running {prediction_type} routing planner...")
    clients = fetch_recent_clients_from_trajectories()
    for client_id in clients:
        try:
            generate_schedule_for_client(client_id, prediction_type)
        except Exception as e:
            logging.error(f"❌ Failed for {client_id}: {e}", exc_info=True)
