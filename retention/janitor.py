import os, time, logging
from datetime import timedelta
from db.db_connection import get_db

logging.basicConfig(level=logging.INFO)

# Tunables
TTL_DAYS = int(os.getenv("RETENTION_TTL_DAYS", "28"))
BATCH_SIZE = int(os.getenv("RETENTION_BATCH_SIZE", "2000"))
SLEEP_SECONDS = int(os.getenv("RETENTION_SLEEP_SECONDS", "5"))

DELETE_SQL = f"""
DELETE FROM trajectories t
USING (
  SELECT ctid
  FROM trajectories
  WHERE created_at < NOW() - INTERVAL '{TTL_DAYS} days'
  ORDER BY created_at
  LIMIT {BATCH_SIZE}
) old
WHERE t.ctid = old.ctid;
"""

COUNT_SQL = f"SELECT COUNT(*) AS n FROM trajectories WHERE created_at < NOW() - INTERVAL '{TTL_DAYS} days';"


def main():
    conn = get_db()
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(COUNT_SQL)
                n = cur.fetchone()["n"]
            if n == 0:
                time.sleep(SLEEP_SECONDS)
                continue

            # chew in small, lock-friendly chunks
            total = 0
            while True:
                with conn.cursor() as cur:
                    cur.execute(DELETE_SQL)
                    deleted = cur.rowcount or 0
                total += deleted
                if deleted < BATCH_SIZE:
                    break

            logging.info(f"🧹 trajectories retention: deleted {total} rows (> {TTL_DAYS}d)")
            # short pause to let autovacuum breathe
            time.sleep(SLEEP_SECONDS)
        except Exception as e:
            logging.error(f"❌ retention loop error: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
