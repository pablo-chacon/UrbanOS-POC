import logging
import time
from db.db_connection import (
    fetch_latest_locations,
    fetch_next_predicted_poi,
    fetch_fallback_stop_point,
    fetch_closest_stop_id,
    save_astar_route,
    fetch_existing_poi_targets
)
from pathfinder import pathfinder

logging.basicConfig(level=logging.INFO)


def main():
    logging.info("🚀 A* Module started.")
    time.sleep(60)  # Grace delay for DB, MQTT, etc.

    while True:
        try:
            clients_with_locations = fetch_latest_locations()
            if not clients_with_locations:
                logging.info("⏳ No valid geodata entries. Sleeping 10s...")
                time.sleep(10)
                continue

            for client_id, lat, lon, speed, updated_at in clients_with_locations:
                latest_location = (lat, lon, speed, updated_at)
                routing_targets = []

                # 1. Route to predicted POI
                poi_df = fetch_next_predicted_poi(client_id)
                if poi_df is not None and not poi_df.empty:
                    poi_lat, poi_lon = poi_df.iloc[0]["lat"], poi_df.iloc[0]["lon"]
                    matching_poi = fetch_existing_poi_targets(client_id, poi_lat, poi_lon)
                    stop_id = fetch_closest_stop_id((poi_lat, poi_lon))  # GTFS-native

                    routing_targets.append({
                        "goal_lat": poi_lat,
                        "goal_lon": poi_lon,
                        "target_type": "poi",
                        "stop_id": stop_id,
                        "parent_station": None,  # could be fetched if needed later
                        "poi_id": matching_poi.get("poi_id") if matching_poi else None,
                        "decision_context": "routed_to_poi"
                    })

                # 2. Fallback to the closest stop point
                fallback = fetch_fallback_stop_point(lat, lon)
                if fallback:
                    fallback_lat, fallback_lon, stop_id = fallback
                    routing_targets.append({
                        "goal_lat": fallback_lat,
                        "goal_lon": fallback_lon,
                        "target_type": "stop_point",
                        "stop_id": stop_id,
                        "parent_station": None,
                        "poi_id": None,
                        "decision_context": "fallback_stop_point"
                    })

                # 3. Process each routing target
                for target in routing_targets:
                    path_gdf = pathfinder(
                        client_id=client_id,
                        goal_lat=target["goal_lat"],
                        goal_lon=target["goal_lon"],
                        latest_location=latest_location
                    )

                    if path_gdf is not None and not path_gdf.empty:
                        efficiency_score = path_gdf["distance"].sum()
                        avg_speed = speed if speed and speed > 0 else 1.4
                        save_astar_route(
                            client_id=client_id,
                            stop_id=target.get("stop_id"),
                            target_type=target["target_type"],
                            parent_station=target.get("parent_station"),
                            poi_id=target.get("poi_id"),
                            origin_coords=(lat, lon),
                            destination_coords=(target["goal_lat"], target["goal_lon"]),
                            path_gdf=path_gdf,
                            speed=avg_speed,
                            decision_context=target["decision_context"],
                            efficiency_score=efficiency_score
                        )

            logging.info("✅ A* cycle complete. Sleeping 10s...")
            time.sleep(10)

        except Exception as e:
            logging.critical(f"💥 A* module crash: {e}", exc_info=True)
            time.sleep(30)


if __name__ == "__main__":
    main()
