import logging
import time
import os
import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

db_connection = None


def get_db_connection(retries=5, delay=5):
    global db_connection
    if db_connection and not db_connection.closed:
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
            logging.info("✅ Connected to PostgreSQL.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ DB connection attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Could not connect to DB after retries.")


def load_from_db(query, conditions=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, conditions if conditions else ())
            return cursor.fetchall()
    except psycopg.Error as e:
        logging.error(f"❌ Query failed: {query}, error: {e}")
        return None


def fetch_latest_trajectories():
    query = "SELECT client_id FROM view_latest_client_trajectories;"
    result = load_from_db(query)
    if not result:
        logging.warning("⚠ No active clients found in geodata.")
        return []
    return [row["client_id"] for row in result]


def save_to_db(table, data):
    if not isinstance(data, dict) or not data:
        logging.error("❌ save_to_db: data must be a non-empty dict.")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            columns, placeholders, values = [], [], []

            for key, value in data.items():
                if key == "geom" and isinstance(value, dict):
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
            cursor.execute(query, values)
            logging.info(f"✅ Saved row to {table}")
    except psycopg.Error as e:
        logging.error(f"❌ save_to_db failed: {e}")
