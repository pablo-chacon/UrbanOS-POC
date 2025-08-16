import logging
import psycopg
import os
import time
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

DB_CONN = None


def get_db_connection(retries=5, delay=5):
    global DB_CONN
    if DB_CONN is not None and not DB_CONN.closed:
        return DB_CONN
    for attempt in range(retries):
        try:
            DB_CONN = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Connected to database.")
            return DB_CONN
        except psycopg.OperationalError as e:
            logging.warning(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Database connection failed after retries.")


def fetch_cluster_departure_candidates():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT *
            FROM "view_cluster_departure_candidates";
        """)
        return cur.fetchall()


def save_cluster_departure_matches(matches):
    if not matches:
        logging.info("⚠ No cluster-departure matches to save.")
        return

    query = """
    INSERT INTO "cluster_departure_matches" (
        client_id, pattern_id, site_gid, line_id, matched_eta, created_at
    )
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT DO NOTHING;
    """

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.executemany(query, matches)
        logging.info(f"✅ Saved {len(matches)} cluster-departure matches.")


def run_cluster_departure_matching():
    logging.info("🔍 Running cluster-departure matching...")
    candidates = fetch_cluster_departure_candidates()

    matches = []
    for row in candidates:
        matches.append((
            row["client_id"],
            row["pattern_id"],
            row["site_gid"],
            row["line_id"],
            row["scheduled"],
        ))

    save_cluster_departure_matches(matches)


if __name__ == "__main__":
    logging.info("🚀 Cluster Departure Matcher started.")
    while True:
        run_cluster_departure_matching()
        logging.info("😴 Sleeping 5 min before next refresh...")
        time.sleep(300)
