import time
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

db_connection = None
logging.basicConfig(level=logging.INFO)


def get_db_connection(retries=5, delay=5):
    global db_connection
    if db_connection is not None:
        return db_connection
    for attempt in range(retries):
        try:
            db_connection = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Connected to DB.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.error(f"Attempt {attempt + 1}: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to DB.")


def load_from_db(query, params=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchall()


# Fetch Tables
def fetch_astar_routes():
    return load_from_db('SELECT * FROM "astar_routes";')


def fetch_mapf_routes():
    return load_from_db('SELECT * FROM "mapf_routes";')


def fetch_trajectories():
    return load_from_db('SELECT * FROM "trajectories";')


def fetch_pois():
    return load_from_db('SELECT * FROM "pois";')


def fetch_predicted_pois_sequence():
    return load_from_db('SELECT * FROM "predicted_pois_sequence";')


def fetch_hotspots():
    return load_from_db('SELECT * FROM "hotspots";')


def fetch_user_patterns():
    return load_from_db('SELECT * FROM "user_patterns";')


def fetch_stop_points():
    return load_from_db('SELECT * FROM "gtfs_stops";')


def fetch_lines():
    return load_from_db('SELECT * FROM "lines";')


def fetch_view_routing_candidates_gtfsrt():
    return load_from_db('SELECT * FROM "view_routing_candidates_gtfsrt";')


def fetch_view_static_gtfs_unified():
    return load_from_db('SELECT * FROM "view_static_gtfs_unified";')


def fetch_view_active_clients_geodata():
    return load_from_db('SELECT * FROM "view_active_clients_geodata";')


def fetch_view_current_session_id_from_geodata():
    return load_from_db('SELECT * FROM "view_current_session_id_from_geodata";')


def fetch_view_astar_eta():
    return load_from_db('SELECT * FROM "view_astar_eta";')


def fetch_view_top_daily_poi():
    return load_from_db('SELECT * FROM "view_top_daily_poi";')


def fetch_view_combined_pois():
    return load_from_db('SELECT * FROM "view_combined_pois";')


def fetch_view_hotspots_heatmap():
    return load_from_db('SELECT * FROM "view_hotspots_heatmap";')


def fetch_view_latest_client_trajectories():
    return load_from_db('SELECT * FROM "view_latest_client_trajectories";')


def fetch_view_departure_candidates():
    return load_from_db('SELECT * FROM "view_departure_candidates";')


def fetch_view_predicted_routes_schedule():
    return load_from_db('SELECT * FROM "view_predicted_routes_schedule";')


def fetch_view_hotspot_overlay():
    return load_from_db('SELECT * FROM "view_hotspot_overlay";')


def fetch_view_daily_routing_summary():
    return load_from_db('SELECT * FROM "view_daily_routing_summary";')


def fetch_view_mapf_active_routes():
    return load_from_db('SELECT * FROM "view_mapf_active_routes";')


def fetch_view_routes_history():
    return load_from_db('SELECT * FROM "view_routes_history";')


def fetch_view_routes_unified_latest():
    return load_from_db('SELECT * FROM "view_routes_unified_latest";')


def fetch_view_routes_live():
    return load_from_db('SELECT * FROM "view_routes_live";')


def fetch_view_routes_unified():
    return load_from_db('SELECT * FROM "view_routes_unified";')


def fetch_view_eta_active_points():
    return load_from_db('SELECT * FROM "view_eta_active_points";')


def fetch_view_pois_nearest_stop():
    return load_from_db('SELECT * FROM "view_pois_nearest_stop";')


def fetch_view_pois_stops_within_300m():
    return load_from_db('SELECT * FROM "view_pois_stops_within_300m";')


def fetch_view_latest_client_routes():
    return load_from_db('SELECT * FROM "view_latest_client_routes";')


def fetch_view_latest_routes_and_trajectories():
    return load_from_db('SELECT * FROM "view_latest_routes_and_trajectories";')


def fetch_view_routes_astar_mapf_unified():
    return load_from_db('SELECT * FROM "view_routes_astar_mapf_unified";')


def fetch_view_routes_astar_mapf_latest():
    return load_from_db('SELECT * FROM "view_routes_astar_mapf_latest";')


def fetch_view_eta_accuracy_seconds():
    return load_from_db('SELECT * FROM "view_eta_accuracy_seconds";')


def fetch_view_boarding_window_hit_rate():
    return load_from_db('SELECT * FROM "view_boarding_window_hit_rate";')


def fetch_view_geodata_latest_point():
    return load_from_db('SELECT * FROM "view_geodata_latest_point";')


def fetch_view_stop_usage_7d():
    return load_from_db('SELECT * FROM "view_stop_usage_7d";')


def fetch_view_predicted_poi_nearest_stop():
    return load_from_db('SELECT * FROM "view_predicted_poi_nearest_stop";')


def fetch_view_client_weekly_schedule_enriched():
    return load_from_db('SELECT * FROM "view_client_weekly_schedule_enriched";')


def fetch_view_next_feasible_departure_per_client():
    return load_from_db('SELECT * FROM "view_next_feasible_departure_per_client";')
