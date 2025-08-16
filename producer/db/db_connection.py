import os
import time
import logging
from typing import List, Dict, Any
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

# Single shared connection
_db = None


def get_db_connection(retries: int = 5, delay: int = 5):
    """Connect to Postgres with basic retry."""
    global _db
    if _db is not None and not _db.closed:
        return _db

    for attempt in range(1, retries + 1):
        try:
            _db = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row,
            )
            logging.info("✅ producer/db: connected to PostgreSQL")
            return _db
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ DB connect attempt {attempt}/{retries} failed: {e}")
            time.sleep(delay)

    raise RuntimeError("❌ producer/db: failed to connect to PostgreSQL after retries")


def load_from_db(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Run a SELECT and return rows as list[dict]."""
    try:
        with get_db_connection().cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as e:
        logging.error(f"❌ DB fetch failed: {e}")
        return []


def fetch_optimized_route() -> List[Dict[str, Any]]:
    """
    Return the freshest unified routes from view_routes_unified.
    We pull rows created in the last ~10 seconds to avoid republishing old history.
    """
    query = """
        SELECT
            client_id,
            stop_id,
            destination_lat,
            destination_lon,
            ST_AsText(path) AS path,
            created_at
        FROM view_routes_unified
        WHERE created_at >= NOW() - INTERVAL '10 seconds'
        ORDER BY created_at DESC;
    """
    return load_from_db(query, ())
