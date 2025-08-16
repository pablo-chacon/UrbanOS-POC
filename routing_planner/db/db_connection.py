import logging
import time
import psycopg
import os
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
        logging.info("✅ DB write ok.")
    except Exception as e:
        logging.error(f"❌ Error saving to database: {e}")


def load_from_db(query, params=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            rows = cursor.fetchall()
        return rows if rows else None
    except Exception as e:
        logging.error(f"❌ Error fetching from database: {e}")
        return None


def fetch_recent_clients_from_trajectories(n=8):
    query = """
    SELECT DISTINCT client_id
    FROM (
        SELECT client_id,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn
        FROM trajectories
    ) sub
    WHERE rn <= %s;
    """
    rows = load_from_db(query, (n,))
    return [r["client_id"] for r in rows] if rows else []


def fetch_predicted_pois_sequence(client_id, prediction_type):
    query = """
    SELECT predicted_lat AS lat,
           predicted_lon AS lon,
           predicted_visit_time AS timestamp
    FROM predicted_pois_sequence
    WHERE client_id = %s AND prediction_type = %s
    ORDER BY predicted_visit_time;
    """
    return load_from_db(query, (client_id, prediction_type))


def fetch_matching_final_route(client_id, lat, lon):
    """
    Use the unified view (optimized_routes + reroutes, latest per destination).
    Return path WKT + segment_type + stop_id for enrichment.
    """
    query = """
    SELECT ST_AsText(path) AS path_text,
           segment_type,
           stop_id
    FROM view_routes_unified
    WHERE client_id = %s
      AND destination_lat = %s
      AND destination_lon = %s
    LIMIT 1;
    """
    rows = load_from_db(query, (client_id, lat, lon))
    return rows[0] if rows else None


def enrich_stop_meta(stop_id):
    """
    Optional: attach route_short_name/long_name if you want schedule niceties.
    """
    if not stop_id:
        return None
    query = """
    SELECT s.stop_id,
           s.stop_name,
           s.platform_code
    FROM gtfs_stops s
    WHERE s.stop_id = %s
    LIMIT 1;
    """
    rows = load_from_db(query, (stop_id,))
    return rows[0] if rows else None


def save_weekly_schedule_entry(client_id, visit_day, predicted_time,
                               lat, lon, path_wkt, segment_type,
                               prediction_type, stop_id=None, stop_name=None, platform_code=None):
    """
    Persist one row in client_weekly_schedule.
    """
    query = """
    INSERT INTO client_weekly_schedule (
        client_id, visit_day, predicted_time,
        poi_lat, poi_lon, path, segment_type, prediction_type
    )
    VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)
    ON CONFLICT DO NOTHING;
    """
    params = (
        client_id, visit_day, predicted_time,
        lat, lon, path_wkt, segment_type, prediction_type
    )
    save_to_db(query, params)
