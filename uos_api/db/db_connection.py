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


def fetch_astar_routes():
    return load_from_db("SELECT * FROM astar_routes;")


def fetch_trajectories():
    return load_from_db("SELECT * FROM trajectories;")


def fetch_pois():
    return load_from_db("SELECT * FROM pois;")


def fetch_predicted_pois_sequence():
    return load_from_db("SELECT * FROM predicted_pois_sequence;")


def fetch_hotspots():
    return load_from_db("SELECT * FROM hotspots;")


def fetch_user_patterns():
    return load_from_db("SELECT * FROM user_patterns;")


def fetch_view_eta_active_points():
    return load_from_db("SELECT * FROM view_eta_active_points;")


def fetch_view_latest_client_trajectories():
    return load_from_db("SELECT * FROM view_latest_client_trajectories;")


def fetch_view_daily_routing_summary():
    return load_from_db("SELECT * FROM view_daily_routing_summary;")


def fetch_view_predicted_routes_schedule():
    return load_from_db("SELECT * FROM view_predicted_routes_schedule;")


def fetch_view_top_daily_poi():
    return load_from_db("SELECT * FROM view_top_daily_poi;")


def fetch_view_astar_eta():
    return load_from_db("SELECT * FROM view_astar_eta;")


def fetch_mapf_routes():
    return load_from_db("SELECT * FROM mapf_routes;")


def fetch_view_mapf_active_routes():
    return load_from_db("SELECT * FROM view_mapf_active_routes;")


def fetch_stop_points():
    return load_from_db("SELECT * FROM gtfs_stops;")


def fetch_lines():
    return load_from_db("SELECT * FROM lines;")
