import logging
from db.db_connection import load_from_db
import client_clusters

logging.basicConfig(level=logging.INFO)


def get_all_clients():
    """
    Fetch all unique client_ids from the trajectories table.
    """
    query = "SELECT DISTINCT client_id FROM trajectories"
    result = load_from_db(query)

    if not result:
        logging.warning("⚠ No clients found in the database.")
        return []

    return [row['client_id'] for row in result]


def process_client_data(client_id):
    """
    Process clustering for a given client_id.
    """
    try:
        logging.info(f"🚀 Starting clustering for client_id: {client_id}")
        client_clusters.process_client_clusters(client_id)
        logging.info(f"✅ Clustering completed for client_id: {client_id}")
    except Exception as e:
        logging.error(f"❌ Error processing clustering for client_id {client_id}: {e}")


def main():
    """
    Entry point for clustering pattern detection for all clients.
    """
    logging.info("🧠 Starting client clustering module...")

    client_ids = get_all_clients()

    if not client_ids:
        logging.info("⚠ No client data found. Exiting.")
        return

    for client_id in client_ids:
        process_client_data(client_id)

    logging.info("✅ Completed clustering for all clients.")


if __name__ == "__main__":
    main()
