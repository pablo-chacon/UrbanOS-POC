import logging
import time
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os
import json
import pandas as pd
import geopandas as gpd
from shapely.wkb import loads

load_dotenv()

db_connection = None


def get_db_connection(retries=5, delay=5):
    global db_connection
    if db_connection is not None:
        return db_connection

    for attempt in range(retries):
        try:
            db_connection = psycopg.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠️ Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to the database after multiple attempts")


def load_from_db(query, conditions=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, conditions or ())
            result = cursor.fetchall()
            if not result:
                logging.warning("⚠ No data found for query: %s", query)
            return result
    except psycopg.Error as e:
        logging.error(f"❌ Error executing query: {query}, Error: {e}")
        return None


def save_to_db(table, data):
    """
    Save data to the specified table. Handles PostGIS geometry ('geom') if present.
    """
    if not isinstance(data, dict) or not data:
        logging.error("❌ Data must be a non-empty dictionary to save to the database.")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            columns = []
            placeholders = []
            values = []

            for key, value in data.items():
                if key == "geom":
                    if isinstance(value, dict) and "lat" in value and "lon" in value:
                        columns.append("geom")
                        placeholders.append("ST_SetSRID(ST_MakePoint(%s, %s), 4326)")
                        values.extend([value["lon"], value["lat"]])
                    elif isinstance(value, str) and value.upper().startswith("LINESTRING"):
                        columns.append("geom")
                        placeholders.append("ST_GeomFromText(%s, 4326)")
                        values.append(value)
                else:
                    columns.append(key)
                    placeholders.append("%s")
                    values.append(value)

            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT DO NOTHING
            """
            cursor.execute(query, tuple(values))
            logging.info(f"✅ Data saved to table '{table}'.")

    except psycopg.Error as e:
        logging.error(f"❌ Error saving to table {table}, Error: {e}")


def load_trajectories(client_id):
    """
    Load all trajectories for a specific client_id and return as GeoDataFrame.
    """
    conn = get_db_connection()
    query = """
    SELECT session_id, trajectory
    FROM trajectories
    WHERE client_id = %s
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (client_id,))
            result = cursor.fetchall()

        if not result:
            logging.warning(f"⚠ No trajectories found for client_id: {client_id}")
            return None

        all_trajectories = []
        for row in result:
            session_id = row.get("session_id")
            trajectory = row.get("trajectory")

            if isinstance(trajectory, str):
                try:
                    trajectory = json.loads(trajectory)
                except json.JSONDecodeError:
                    logging.error(f"❌ Invalid JSON in trajectory for client_id: {client_id}")
                    continue

            if isinstance(trajectory, list):
                df = pd.DataFrame(trajectory)
                if {'lat', 'lon', 'timestamp', 'speed'}.issubset(df.columns):
                    df["client_id"] = client_id
                    df["session_id"] = session_id
                    all_trajectories.append(df)
                else:
                    logging.warning(f"⚠ Missing required fields in session {session_id}")
            else:
                logging.error(f"⚠ Invalid format in session {session_id}")

        if not all_trajectories:
            logging.warning(f"⚠ No valid trajectories for client_id: {client_id}")
            return None

        df = pd.concat(all_trajectories, ignore_index=True)
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
        logging.info(f"✅ Loaded {len(gdf)} trajectory points for client_id: {client_id}")
        return gdf

    except psycopg.Error as e:
        logging.error(f"❌ Error loading trajectories for client_id: {client_id}, Error: {e}")
        return None


def load_stop_points():
    """
    Load all static stop_points from the DB.
    """
    query = "SELECT stop_lat, stop_lon FROM gtfs_stops;"
    return load_from_db(query)


def fetch_trajectory_clients():
    """
    Fetch distinct client_ids that have data in the `trajectories` table.
    """
    query = "SELECT DISTINCT client_id FROM trajectories;"
    result = load_from_db(query)
    if not result:
        logging.warning("⚠ No clients found in trajectories.")
        return []
    return [row["client_id"] for row in result]
