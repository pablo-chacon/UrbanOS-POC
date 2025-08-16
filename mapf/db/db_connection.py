import logging
import time
import psycopg
import os
import pandas as pd
import geopandas as gpd
from psycopg.rows import dict_row
from dotenv import load_dotenv
from shapely.wkb import loads

load_dotenv()
db_connection = None


def get_db_connection(retries=5, delay=5):
    global db_connection
    if db_connection and not db_connection.closed:
        return db_connection

    for attempt in range(retries):
        try:
            db_connection = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ Attempt {attempt + 1}: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to database.")


def load_from_db(query, params=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ Error loading from DB: {e}")
        return None


def save_to_db(query, params):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
        logging.info("✅ Saved MAPF route.")
    except Exception as e:
        logging.error(f"❌ Error saving MAPF route: {e}")


def fetch_active_clients():
    query = "SELECT client_id FROM view_active_clients_geodata;"
    result = load_from_db(query)
    return [row["client_id"] for row in result] if result else []


def fetch_latest_location(client_id):
    query = """
    SELECT lat, lon, updated_at
    FROM geodata
    WHERE client_id = %s
    ORDER BY updated_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    return (result[0]["lat"], result[0]["lon"], result[0]["updated_at"]) if result else None


def fetch_next_predicted_poi(client_id):
    query = """
    SELECT predicted_lat AS lat, predicted_lon AS lon, predicted_visit_time AS timestamp,
           ST_AsBinary(geom) AS geom
    FROM view_top_daily_poi
    WHERE client_id = %s
    ORDER BY predicted_visit_time
    LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    if not result:
        return gpd.GeoDataFrame(columns=["lat", "lon", "timestamp", "geom"])
    df = pd.DataFrame(result)
    df["geom"] = df["geom"].apply(lambda x: loads(x) if x else None)
    return gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")


def fetch_fallback_stop():
    query = """
    SELECT stop_lat AS lat, stop_lon AS lon
    FROM gtfs_stops
    WHERE location_type = 0
    ORDER BY RANDOM()
    LIMIT 1;
    """
    result = load_from_db(query)
    return (result[0]["lat"], result[0]["lon"]) if result else None


def fetch_astar_target(client_id):
    query = """
    SELECT destination_lat, destination_lon, target_type, stop_id
    FROM astar_routes
    WHERE client_id = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    return result[0] if result else None


def fetch_astar_path(client_id, destination):
    query = """
    SELECT ST_AsBinary(path) AS path
    FROM astar_routes
    WHERE client_id = %s
      AND destination_lat = %s AND destination_lon = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, destination[0], destination[1]))
    if result:
        return list(loads(result[0]["path"]).coords)
    return None
