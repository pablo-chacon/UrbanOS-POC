import time
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Global connection pool
db_connection = None


def get_db_connection(retries=5, delay=5):
    """Returns a persistent PostgreSQL database connection with retry logic."""
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
            logging.info("Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.error(f"Database connection attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)

    raise Exception("Failed to connect to the database after multiple attempts")


def insert_data(trajectory_data=None):
    """Inserts trajectory data into the database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if trajectory_data:
                valid_data = []

                for data in trajectory_data:
                    session_id = data[0]  # Extract session_id
                    client_id = data[1]  # Extract client_id

                    # Check if session_id exists
                    cur.execute("SELECT session_id FROM mqtt_sessions WHERE session_id = %s", (session_id,))
                    result = cur.fetchone()

                    if not result:
                        logging.warning(
                            f"Session ID {session_id} does not exist in mqtt_sessions. Skipping this entry.")
                    else:
                        valid_data.append(data)  # Only add valid data entries

                if valid_data:
                    cur.executemany(
                        """
                        INSERT INTO geodata (
                        session_id, client_id, lat, lon, elevation, speed, activity, timestamp, geom)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        valid_data
                    )
                    conn.commit()
                    logging.info(f"Inserted {len(valid_data)} trajectory points into geodata.")

    except Exception as e:
        logging.error(f"Database insertion error: {e}")
