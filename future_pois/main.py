import os
import logging
import time
from db.db_connection import fetch_latest_trajectories, load_from_db
from predict_pois import predict_next_poi, get_poi_and_patterns

logging.basicConfig(level=logging.INFO)

ENABLE_WEEKLY = os.getenv("FUTURE_POIS_ENABLE_WEEKLY", "1") == "1"
WEEKLY_INTERVAL_SEC = int(os.getenv("FUTURE_POIS_WEEKLY_INTERVAL_SEC", str(12 * 3600)))  # every 12h by default
_last_weekly_ts = 0


def main():
    logging.info("🚀 future_pois cycle starting…")
    time.sleep(10)

    global _last_weekly_ts
    while True:
        clients = fetch_latest_trajectories()
        now = time.time()
        run_weekly = ENABLE_WEEKLY and (now - _last_weekly_ts >= WEEKLY_INTERVAL_SEC)

        for client_id in clients:
            # Daily: always try; fall back handled downstream if sequence ends up empty.
            logging.info(f"🧠 Daily predict → {client_id}")
            seq_daily = predict_next_poi(client_id, "daily", get_poi_and_patterns)
            if not seq_daily:
                logging.info(f"ℹ️ {client_id}: no POI/pattern signal yet — routing will use stop-point fallback.")

            # Weekly: only on cadence (self-healing)
            if run_weekly:
                logging.info(f"🧠 Weekly predict → {client_id}")
                _ = predict_next_poi(client_id, "weekly", get_poi_and_patterns)

        if run_weekly:
            _last_weekly_ts = now

        logging.info("✅ Prediction sweep done. Sleeping 5 minutes.")
        time.sleep(300)


if __name__ == "__main__":
    main()
