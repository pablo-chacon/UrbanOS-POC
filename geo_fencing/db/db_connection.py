import logging
import os
import time

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

db_connection = None


def get_db_connection(retries=5, delay=5):
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
                port=os.getenv('POSTGRES_PORT', '5432'),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to the database after multiple attempts")


def save_to_db(query, params):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
        logging.info("✅ Query executed successfully.")
    except Exception as e:
        logging.error(f"❌ DB write failed: {e}")


def load_from_db(query, params=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ DB fetch failed: {e}")
        return None


def fetch_active_clients():
    query = "SELECT client_id FROM view_active_clients_geodata;"
    result = load_from_db(query)
    if not result:
        logging.warning("⚠ No active clients found in geodata.")
        return []
    return [row["client_id"] for row in result]


def fetch_trajectory_points(client_id: str) -> list[tuple[float, float]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT "trajectory"
                FROM "trajectories"
                WHERE "client_id" = %s
                ORDER BY "created_at" DESC
                LIMIT 1
            """, (client_id,))
            row = cur.fetchone()
            if not row:
                return []
            traj_json = row['trajectory']
            return [(point['lon'], point['lat']) for point in traj_json if 'lat' in point and 'lon' in point]


def save_geo_fence(client_id: str, polygon_wkt: str, location_name: str = None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO "geo_fence" ("client_id", "geo_fence", "location_name")
                VALUES (%s, ST_GeomFromText(%s, 4326), %s)
                ON CONFLICT ("client_id")
                DO UPDATE SET "geo_fence" = EXCLUDED."geo_fence", "location_name" = EXCLUDED."location_name"
            """, (client_id, polygon_wkt, location_name))
