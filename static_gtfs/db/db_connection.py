import time
import psycopg
import os
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()


def get_db_connection(retries=5, delay=5):
    for attempt in range(retries):
        try:
            db_host = os.getenv('POSTGRES_HOST')
            print(f"Connecting to database at host: {db_host}")
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


def save_bulk(table_name: str, rows: list[dict], conflict_action="NOTHING"):
    if not rows:
        print(f"⚠️ Skipping {table_name}: empty data")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            columns = rows[0].keys()
            col_str = ', '.join(f'"{col}"' for col in columns)
            val_str = ', '.join([f'%({col})s' for col in columns])
            query = f"""
                INSERT INTO "{table_name}" ({col_str})
                VALUES ({val_str})
                ON CONFLICT DO {conflict_action};
            """
            cursor.executemany(query, rows)
        print(f"✅ Bulk inserted {len(rows)} rows into {table_name}")
    except Exception as e:
        print(f"❌ DB bulk insert failed for {table_name}: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_routes(rows):
    if not rows:
        print("⚠️ Skipping gtfs_routes: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_routes" (
                    "route_id", "agency_id", "route_short_name",
                    "route_long_name", "route_type", "route_desc"
                ) VALUES (
                    %(route_id)s, %(agency_id)s, %(route_short_name)s,
                    %(route_long_name)s, %(route_type)s, %(route_desc)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_routes")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_routes: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_calendar(rows):
    if not rows:
        print("⚠️ Skipping gtfs_calendar: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_calendar" (
                    "service_id", "monday", "tuesday", "wednesday",
                    "thursday", "friday", "saturday", "sunday",
                    "start_date", "end_date"
                ) VALUES (
                    %(service_id)s, %(monday)s, %(tuesday)s, %(wednesday)s,
                    %(thursday)s, %(friday)s, %(saturday)s, %(sunday)s,
                    %(start_date)s, %(end_date)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_calendar")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_calendar: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_calendar_dates(rows):
    if not rows:
        print("⚠️ Skipping gtfs_calendar_dates: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_calendar_dates" (
                    "service_id", "date", "exception_type"
                ) VALUES (
                    %(service_id)s, %(date)s, %(exception_type)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_calendar_dates")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_calendar_dates: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_stops(rows):
    if not rows:
        print("⚠️ Skipping gtfs_stops: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_stops" (
                    "stop_id", "stop_code", "stop_name", "stop_desc",
                    "stop_lat", "stop_lon", "zone_id", "stop_url",
                    "location_type", "parent_station", "stop_timezone",
                    "wheelchair_boarding", "platform_code"
                ) VALUES (
                    %(stop_id)s, %(stop_code)s, %(stop_name)s, %(stop_desc)s,
                    %(stop_lat)s, %(stop_lon)s, %(zone_id)s, %(stop_url)s,
                    %(location_type)s, %(parent_station)s, %(stop_timezone)s,
                    %(wheelchair_boarding)s, %(platform_code)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_stops")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_stops: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_trips(rows):
    if not rows:
        print("⚠️ Skipping gtfs_trips: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_trips" (
                    "trip_id", "route_id", "service_id",
                    "trip_headsign", "direction_id", "shape_id"
                ) VALUES (
                    %(trip_id)s, %(route_id)s, %(service_id)s,
                    %(trip_headsign)s, %(direction_id)s, %(shape_id)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_trips")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_trips: {e}")
        raise
    finally:
        conn.close()


def save_gtfs_stop_times(rows):
    if not rows:
        print("⚠️ Skipping gtfs_stop_times: no rows")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO "gtfs_stop_times" (
                    "trip_id", "arrival_time", "departure_time", "stop_id",
                    "stop_sequence", "stop_headsign", "pickup_type",
                    "drop_off_type", "shape_dist_traveled", "timepoint",
                    "pickup_booking_rule_id", "drop_off_booking_rule_id"
                ) VALUES (
                    %(trip_id)s, %(arrival_time)s, %(departure_time)s, %(stop_id)s,
                    %(stop_sequence)s, %(stop_headsign)s, %(pickup_type)s,
                    %(drop_off_type)s, %(shape_dist_traveled)s, %(timepoint)s,
                    %(pickup_booking_rule_id)s, %(drop_off_booking_rule_id)s
                ) ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, rows)
        print(f"✅ Inserted {len(rows)} rows into gtfs_stop_times")
    except Exception as e:
        print(f"❌ Insert failed for gtfs_stop_times: {e}")
        raise
    finally:
        conn.close()
