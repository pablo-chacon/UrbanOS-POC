import os
import time
from dotenv import load_dotenv
from gtfs_rt.gtfs_loader import fetch_gtfs_realtime
from db.db_connection import (
    save_trip_updates,
    save_vehicle_positions,
    save_service_alerts
)

# Load .env
load_dotenv()
GTFS_KEY = os.getenv("GTFS_RT_KEY")
GTFS_BASE_URL = "https://opendata.samtrafiken.se/gtfs-rt/sl"

GTFS_ENDPOINTS = {
    "vehicle_positions": f"{GTFS_BASE_URL}/VehiclePositions.pb?key={GTFS_KEY}",
    "trip_updates": f"{GTFS_BASE_URL}/TripUpdates.pb?key={GTFS_KEY}",
    "service_alerts": f"{GTFS_BASE_URL}/ServiceAlerts.pb?key={GTFS_KEY}"
}

SAVE_FUNCTIONS = {
    "vehicle_positions": save_vehicle_positions,
    "trip_updates": save_trip_updates,
    "service_alerts": save_service_alerts
}

# --- Interval Config ---
GTFS_INTERVAL_SECONDS = 60  # Real-time GTFS: every ~1 minute


def update_gtfs_realtime_data():
    print("🔁 Fetching GTFS-RT feeds...")
    for data_type, url in GTFS_ENDPOINTS.items():
        try:
            rows = fetch_gtfs_realtime(url, data_type)
            if not rows:
                print(f"⚠️ No rows parsed for {data_type}")
                continue

            SAVE_FUNCTIONS[data_type](rows)
            print(f"✅ {len(rows)} rows stored to {data_type}")
        except Exception as e:
            print(f"❌ Error fetching {data_type}: {e}")


def main():
    print("🚀 RTD module started.")

    while True:
        update_gtfs_realtime_data()
        print(f"😴 Sleeping {GTFS_INTERVAL_SECONDS}s...\n")
        time.sleep(GTFS_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
