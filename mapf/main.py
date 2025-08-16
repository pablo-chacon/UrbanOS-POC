import logging
import time
from db.db_connection import (
    fetch_active_clients,
    fetch_astar_target,
    fetch_astar_path,
)
from mapf_engine import run_mapf_for_client

logging.basicConfig(level=logging.INFO)


def main():
    logging.info("🚀 MAPF Module started.")
    time.sleep(60)  # startup grace

    while True:
        clients = fetch_active_clients()
        if not clients:
            logging.info("⏳ No active clients. Sleeping...")
            time.sleep(60)
            continue

        for client_id in clients:
            try:
                target = fetch_astar_target(client_id)
                if not target:
                    logging.info(f"⚠ No A* target for {client_id}")
                    continue

                destination = (target["destination_lat"], target["destination_lon"])
                stop_id = target.get("stop_id")  # ✅ GTFS stop_id from astar_routes

                if not stop_id:
                    logging.warning(f"⚠ No stop_id found for {client_id}")
                    continue

                run_mapf_for_client(client_id, destination, stop_id)

            except Exception as e:
                logging.error(f"❌ MAPF error for {client_id}: {e}", exc_info=True)

        logging.info("😴 MAPF cycle complete. Sleeping 60s...")
        time.sleep(60)


if __name__ == "__main__":
    main()
