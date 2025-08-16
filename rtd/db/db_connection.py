import time
import psycopg
import os
from psycopg.rows import dict_row


def get_db_connection(retries=5, delay=5):
    for attempt in range(retries):
        try:
            db_host = os.getenv('POSTGRES_HOST')
            print(f"Connecting to database at host: {db_host}")  # Debug line
            conn = psycopg.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=db_host,
                port=os.getenv('POSTGRES_PORT'),
                autocommit=True,
                row_factory=dict_row
            )
            return conn
        except psycopg.OperationalError as e:
            print(f"Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception("Failed to connect to the database after multiple attempts")


def save_to_db(table, data, conflict_action="NOTHING"):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if isinstance(data, dict):
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                data_values = tuple(data.values())
            else:
                # Assuming data is a tuple or list with values in order
                columns = ', '.join([f"col{i + 1}" for i in range(len(data))])  # Replace colX with actual column names
                values = ', '.join(['%s'] * len(data))
                data_values = data

            query = f"""
                INSERT INTO {table} ({columns}) 
                VALUES ({values})
                ON CONFLICT DO {conflict_action}
            """
            cursor.execute(query, data_values)
    finally:
        conn.close()


def load_from_db(query, conditions=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if conditions:
                # Adjusting to handle both tuple and dict for conditions
                params = tuple(conditions.values()) if isinstance(conditions, dict) else conditions
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    finally:
        conn.close()


def save_trip_updates(rows):
    if not rows:
        return

    conn = get_db_connection()
    query = """
        INSERT INTO trip_updates (
            trip_id, stop_id, arrival_time, departure_time,
            delay_seconds, status, created_at
        )
        VALUES (%(trip_id)s, %(stop_id)s, %(arrival_time)s, %(departure_time)s,
                %(delay_seconds)s, %(status)s, %(created_at)s)
        ON CONFLICT DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)
        conn.commit()
    finally:
        conn.close()


def save_vehicle_positions(rows):
    if not rows:
        return

    conn = get_db_connection()
    query = """
        INSERT INTO vehicle_arrivals (
            vehicle_id, trip_id, route_id, position_lat, position_lon,
            stop_id, timestamp, created_at
        )
        VALUES (%(vehicle_id)s, %(trip_id)s, %(route_id)s, %(lat)s, %(lon)s,
                %(stop_id)s, %(timestamp)s, %(created_at)s)
        ON CONFLICT DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)
        conn.commit()
    finally:
        conn.close()


def save_service_alerts(rows):
    if not rows:
        return

    conn = get_db_connection()
    query = """
        INSERT INTO service_alerts (
            alert_id, cause, effect, header_text, description_text,
            affected_entity, start_time, end_time, created_at
        )
        VALUES (%(alert_id)s, %(cause)s, %(effect)s, %(header_text)s, %(description_text)s,
                %(affected_entity)s, %(start_time)s, %(end_time)s, %(created_at)s)
        ON CONFLICT (alert_id) DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)
        conn.commit()
    finally:
        conn.close()
