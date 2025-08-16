import os, time, logging, psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()
_conn = None


def get_db(retries=5, delay=5):
    global _conn
    if _conn and not _conn.closed:
        return _conn
    for i in range(retries):
        try:
            _conn = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row,
            )
            logging.info("✅ retention connected to PostgreSQL")
            return _conn
        except Exception as e:
            logging.warning(f"⚠ DB connect attempt {i + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise RuntimeError("❌ retention: cannot connect to DB")
