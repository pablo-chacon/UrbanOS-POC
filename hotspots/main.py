import logging
from db.db_connection import (
    fetch_historical_trajectories,
    fetch_pois,
    insert_hotspots
)
from hotspot_detection import detect_hotspots, expand_trajectories

logging.basicConfig(level=logging.INFO)


def process_hotspots():
    logging.info("\U0001F680 Fetching input vectors for hotspot detection...")

    # Historical movement base
    historical_data = fetch_historical_trajectories()
    trajectory_points = expand_trajectories(historical_data, source_type="trajectory")

    # Points of Interest
    poi_data = fetch_pois()

    # Combine input sources
    combined_data = trajectory_points + poi_data

    if not combined_data:
        logging.warning("\u26a0 No data available for hotspot detection.")
        return

    hotspots = detect_hotspots(combined_data)

    if not hotspots:
        logging.warning("\u26a0 No hotspots detected.")
        return

    insert_hotspots(hotspots)
    logging.info("\u2705 Hotspot processing completed.")


if __name__ == "__main__":
    logging.info("\U0001F680 Running Hotspot Detection Module...")
    process_hotspots()