import logging
import time
import psycopg
from geoalchemy2.shape import to_shape
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os

load_dotenv()

# Global connection pool
db_connection = None

# Configure logging
logging.basicConfig(level=logging.INFO)


def get_db_connection(retries=5, delay=5):
    """Establishes a persistent PostgreSQL connection with retry logic."""
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
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.error(f"❌ Database connection attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)

    raise Exception("❌ Failed to connect to the database after multiple attempts")


def load_from_db(query, params=None):
    """Executes a SELECT query and returns the results."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ Error executing query: {query} | Error: {e}")
        return []


def fetch_geodata_for_hotspots():
    """
    Fetches recent geodata for real-time hotspot detection.
    """
    query = """
    SELECT client_id, lat, lon, timestamp 
    FROM geodata
    WHERE timestamp >= NOW() - INTERVAL '24 HOURS'
    ORDER BY timestamp DESC;
    """
    return load_from_db(query)


def fetch_historical_trajectories():
    """
    Fetches trajectory data from the `trajectories` table for historical hotspot detection.
    """
    query = """
    SELECT client_id, trajectory
    FROM trajectories;
    """  # ✅ Directly fetch lat/lon without extra processing
    return load_from_db(query)


def insert_hotspots(hotspots):
    """
    Inserts detected hotspots into the `hotspots` table.
    """
    query = """
    INSERT INTO hotspots (client_id, lat, lon, radius, density, type, geom, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326), NOW())
    ON CONFLICT (client_id, lat, lon) DO UPDATE 
    SET radius = EXCLUDED.radius, density = EXCLUDED.density, updated_at = NOW();
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for hotspot in hotspots:
                geom_wkt = to_shape(hotspot["geom"]).wkt
                cursor.execute(query, (
                    hotspot["client_id"],
                    hotspot["lat"],
                    hotspot["lon"],
                    hotspot["radius"],
                    hotspot["density"],
                    hotspot.get("type", "hotspot"),
                    geom_wkt
                ))
        logging.info("✅ Hotspot data inserted into database.")
    except Exception as e:
        logging.error(f"❌ Error saving hotspot data: {e}")


def fetch_pois(source_type="poi"):
    query = """
    SELECT client_id, lat, lon
    FROM pois
    WHERE lat IS NOT NULL AND lon IS NOT NULL;
    """
    rows = load_from_db(query)
    return [
        {
            "client_id": row["client_id"],
            "lat": row["lat"],
            "lon": row["lon"],
            "source_type": source_type
        } for row in rows
    ]
