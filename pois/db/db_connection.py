import json
import logging
import time
import numpy as np
import pandas as pd
import psycopg
import os
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from sklearn.cluster import DBSCAN

# Load environment variables
load_dotenv()

# Global variable for storing the database connection
db_connection = None


def get_db_connection(retries=5, delay=5):
    """
    Establish and return a PostgreSQL database connection with retry logic.
    """
    global db_connection
    if db_connection is not None and not db_connection.closed:
        return db_connection

    for attempt in range(retries):
        try:
            db_connection = psycopg.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT', '5432'),  # ✅ Default to 5432 if not set
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to the database after multiple attempts")


def load_from_db(query, conditions=None):
    """
    Load data from the database using a given query and optional conditions.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if conditions:
                cursor.execute(query, conditions)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            if not result:
                logging.warning("No data found for query: %s", query)
            return result
    except psycopg.Error as e:
        logging.error(f"Error executing query: {query}, Error: {e}")
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
                if key == "geom" and isinstance(value, dict) and "lat" in value and "lon" in value:
                    columns.append("geom")
                    placeholders.append("ST_SetSRID(ST_MakePoint(%s, %s), 4326)")
                    values.extend([value["lon"], value["lat"]])
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


def fetch_client_poi_count(client_id):
    """
    Count existing POIs for the given client_id.
    """
    query = "SELECT COUNT(*) FROM pois WHERE client_id = %s;"
    result = load_from_db(query, (client_id,))
    return result[0]["count"] if result else 0


def save_pois_to_db(pois_df):
    for _, row in pois_df.iterrows():
        if any(pd.isna(row[col]) for col in ['client_id', 'lat', 'lon', 'time_spent', 'poi_rank']):
            logging.error(f"Skipping invalid POI row: {row}")
            continue

        data = {
            'client_id': row['client_id'],
            'lat': row['lat'],
            'lon': row['lon'],
            'geom': {"lat": row['lat'], "lon": row['lon']},
            'time_spent': row['time_spent'],
            'poi_rank': row['poi_rank'],
            'created_at': row['poi_created_at'] if 'poi_created_at' in row and pd.notnull(
                row['poi_created_at']) else pd.Timestamp.now()
        }

        try:
            save_to_db('pois', data)
            logging.info(f"✅ Saved POI: {data}")
        except Exception as e:
            logging.error(f"❌ Error saving POI data: {e}")


def load_client_trajectories(client_id):
    """
    Load and unpack all trajectories for a specific client_id using Python-side JSON decoding.
    """
    query = """
        SELECT session_id, trajectory
        FROM trajectories
        WHERE client_id = %s
        """

    result = load_from_db(query, (client_id,))
    if not result:
        logging.warning(f"⚠ No trajectory data found for client_id {client_id}.")
        return pd.DataFrame()

    all_trajectories = []

    for row in result:
        session_id = row.get("session_id")
        trajectory = row.get("trajectory")

        # Ensure proper JSON list decoding
        if isinstance(trajectory, str):
            try:
                trajectory = json.loads(trajectory)
            except json.JSONDecodeError:
                logging.error(f"❌ Invalid JSON format for client {client_id} in session {session_id}")
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
            logging.warning(f"⚠ Non-list trajectory in session {session_id}")

    if not all_trajectories:
        logging.warning(f"⚠ No valid trajectory records for client_id: {client_id}")
        return pd.DataFrame()

    df = pd.concat(all_trajectories, ignore_index=True)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["speed"] = df["speed"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    logging.info(f"✅ Loaded {len(df)} trajectory points for client_id: {client_id}")
    return df


def update_poi_arrival(client_id: str, lat: float, lon: float, visit_start):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pois
                SET
                  visit_count = COALESCE(visit_count, 0) + 1,
                  visit_start = GREATEST(COALESCE(visit_start, to_timestamp(0)::timestamp), %s)
                WHERE client_id = %s AND lat = %s AND lon = %s
                """,
                (visit_start, client_id, lat, lon)
            )
            if cursor.rowcount == 0:
                logging.warning(
                    "⚠ No matching POI for arrival update (client_id=%s, lat=%s, lon=%s)",
                    client_id, lat, lon
                )
    except psycopg.Error as e:
        logging.error(f"❌ update_poi_arrival failed: {e}")
