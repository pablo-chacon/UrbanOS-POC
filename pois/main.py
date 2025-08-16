import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from db.db_connection import fetch_trajectory_clients
from poi_op import process_client_pois

logging.basicConfig(level=logging.INFO)


def process_client(client_id):
    """
    Run POI detection for a single client.
    """
    logging.info(f"🚀 Running POI detection for client_id: {client_id}")
    try:
        process_client_pois(client_id)
    except Exception as e:
        logging.error(f"❌ Error processing client {client_id}: {e}")


def main():
    logging.info("🧠 Starting POI detection process...")

    client_ids = fetch_trajectory_clients()
    if not client_ids:
        logging.warning("⚠ No active clients found in geodata. Aborting.")
        return

    max_workers = min(len(client_ids), 50)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_client, client_id): client_id
                for client_id in client_ids
            }

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"❌ Thread error for client {futures[future]}: {e}")

        logging.info("✅ Finished POI detection for all clients.")
    except Exception as e:
        logging.error(f"❌ Fatal error in POI module: {e}")


if __name__ == "__main__":
    main()
