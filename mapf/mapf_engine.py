import logging
from shapely.geometry import LineString
from cbs import CBSSolver
from db.db_connection import save_to_db

logging.basicConfig(level=logging.INFO)


def run_mapf_for_client(client_id, destination_coords, stop_id=None):
    logging.info(f"🧠 Running MAPF for client_id={client_id}, stop_id={stop_id}...")

    try:
        cbs_solver = CBSSolver(
            client_id=client_id,
            goals=[destination_coords],
            get_sum_of_cost=lambda paths: sum(len(p) for p in paths),
            max_time=10
        )
        paths = cbs_solver.find_solution()
        if not paths or not paths[0]:
            logging.warning(f"⚠ MAPF returned no path for {client_id}")
            return

        linestring = LineString(paths[0])
        query = """
        INSERT INTO mapf_routes (
            client_id, stop_id, destination_lat, destination_lon, path,
            success, decision_context, created_at
        )
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326), TRUE, 'mapf_predicted', NOW())
        ON CONFLICT DO NOTHING;
        """
        lat, lon = destination_coords
        save_to_db(query, (client_id, stop_id, lat, lon, linestring.wkt))
        logging.info(f"✅ MAPF route saved for {client_id}")

    except Exception as e:
        logging.error(f"❌ MAPF engine crash: {e}", exc_info=True)
