import time
import logging
from db.db_connection import fetch_active_clients, fetch_trajectory_points, save_geo_fence
from utility.geo_logic import compute_geo_fence

logging.basicConfig(level=logging.INFO)

SLEEP_SECONDS = 300  # Run every 5 minutes


def main():
    logging.info("🌐 Geo-Fencing Module started (autonomous loop mode)")
    time.sleep(15)  # Give time for DB to stabilize

    while True:
        try:
            clients = fetch_active_clients()
            if not clients:
                logging.info("⏳ No active clients. Sleeping...")
                time.sleep(SLEEP_SECONDS)
                continue

            for client_id in clients:
                try:
                    trajectory_points = fetch_trajectory_points(client_id)
                    if not trajectory_points:
                        logging.warning(f"⚠ No trajectory data found for {client_id}")
                        continue

                    geo_fence = compute_geo_fence(trajectory_points)
                    if geo_fence:
                        save_geo_fence(client_id, geo_fence)
                        logging.info(f"✅ Geo-fence updated for {client_id}")
                    else:
                        logging.warning(f"⚠ Unable to compute geo-fence for {client_id}")

                except Exception as e:
                    logging.error(f"❌ Error processing client {client_id}: {e}", exc_info=True)

        except Exception as loop_error:
            logging.critical(f"💥 Critical loop error: {loop_error}", exc_info=True)

        logging.info(f"😴 Sleeping {SLEEP_SECONDS}s before next geo-fence update cycle...")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
