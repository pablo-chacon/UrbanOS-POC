import time
import logging
import pandas as pd
from collections import defaultdict
from db.db_connection import (
    fetch_migratable_sessions_and_data,
    save_trajectories,
    delete_migrated_geodata_by_session_keys
)

logging.basicConfig(level=logging.INFO)


def migrate_geodata_to_trajectories():
    logging.info("🔁 Sovereign migration triggered...")

    rows = fetch_migratable_sessions_and_data()
    if not rows:
        logging.info("✅ No migratable geodata found.")
        return

    grouped = defaultdict(lambda: defaultdict(list))  # grouped[client_id][session_id] = []

    for row in rows:
        grouped[row["client_id"]][row["session_id"]].append({
            "lat": row["lat"],
            "lon": row["lon"],
            "elevation": row["elevation"],
            "speed": row["speed"],
            "activity": row["activity"],
            "timestamp": pd.to_datetime(row["timestamp"]).isoformat()
        })

    trajectories = [
        (client_id, session_id, points)
        for client_id, sessions in grouped.items()
        for session_id, points in sessions.items()
    ]

    if not trajectories:
        logging.info("⚠ No valid grouped data to save.")
        return

    save_trajectories(trajectories)

    session_keys = [
        (client_id, session_id)
        for client_id in grouped
        for session_id in grouped[client_id]
    ]
    delete_migrated_geodata_by_session_keys(session_keys)

    logging.info(f"✅ Migrated and cleared {len(session_keys)} sessions.")


if __name__ == "__main__":
    logging.info("🚀 Geodata Processor started.")
    time.sleep(35)
    while True:
        migrate_geodata_to_trajectories()
        logging.info("😴 Sleeping 5 min...")
        time.sleep(300)
