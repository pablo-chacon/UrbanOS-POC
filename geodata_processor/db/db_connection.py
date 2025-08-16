import os
import time
import json
import logging
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
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Connected to DB.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠️ Attempt {attempt + 1}: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to the database after retries.")


def load_from_db(query, params=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchall()


def fetch_migratable_sessions_and_data():
    query = """
    SELECT 
        g.client_id, g.session_id,
        g.lat, g.lon, g.elevation, g.speed, g.activity, g.timestamp
    FROM geodata g
    JOIN mqtt_sessions m
      ON g.client_id = m.client_id AND g.session_id = m.session_id
    WHERE g.timestamp BETWEEN m.start_time AND m.end_time
    ORDER BY g.client_id, g.session_id, g.timestamp;
    """
    return load_from_db(query)


def save_trajectories(data):
    if not data:
        logging.warning("⚠ No trajectory data to save.")
        return

    conn = get_db_connection()
    with conn.cursor() as cursor:
        for client_id, session_id, trajectory in data:
            try:
                cursor.execute("""
                    INSERT INTO trajectories (client_id, session_id, trajectory, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (session_id) DO NOTHING;
                """, (client_id, session_id, json.dumps(trajectory)))
            except Exception as e:
                logging.error(f"❌ Failed to save trajectory for {client_id}, session {session_id}: {e}")
        conn.commit()
    logging.info(f"✅ Saved {len(data)} trajectory session(s) to the database.")


def delete_migrated_geodata_by_session_keys(session_keys):
    """
    Delete geodata rows once they're migrated.
    :param session_keys: List of (client_id, session_id)
    """
    if not session_keys:
        logging.info("✅ No migrated geodata to delete.")
        return

    conn = get_db_connection()
    with conn.cursor() as cursor:
        placeholders = ",".join(["(%s, %s)"] * len(session_keys))
        flat_params = [item for pair in session_keys for item in pair]

        query = f"""
            DELETE FROM geodata
            WHERE (client_id, session_id) IN ({placeholders});
        """
        cursor.execute(query, flat_params)
        conn.commit()

    logging.info(f"🧹 Cleared migrated geodata for {len(session_keys)} sessions.")

