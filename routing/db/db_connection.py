import logging
import time
import os
import psycopg
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
from psycopg.rows import dict_row
from shapely import wkb, wkt
import numpy as np
from datetime import datetime
from tensorflow import keras

# cached model
_lstm_model = None
_lstm_loaded = False
MODEL_PATH = "/app/saved_models/lstm_model.keras"
MODEL_WEIGHTS = "/app/saved_models/lstm_model.weights.h5"

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
            logging.info("✅ Connected to database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ DB connect attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to database after retries.")


def load_from_db(query, params=None):
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ DB fetch failed: {e}")
        return None


def save_to_db(query, params):
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query, params)
        logging.info("✅ DB write successful.")
    except Exception as e:
        logging.error(f"❌ DB write failed: {e}")


def fetch_active_clients():
    query = "SELECT client_id FROM view_active_clients_geodata;"
    result = load_from_db(query)
    if not result:
        logging.warning("⚠ No active clients found in geodata.")
        return []
    return [row["client_id"] for row in result]


def fetch_latest_location(client_id):
    query = """
    SELECT lat, lon, updated_at
    FROM geodata
    WHERE client_id = %s
      AND updated_at <= NOW() - INTERVAL '2 seconds'
    ORDER BY updated_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    return (result[0]['lat'], result[0]['lon'], result[0]['updated_at']) if result else None


def fetch_daily_predicted_pois(client_id):
    query = """
    SELECT predicted_lat AS lat, predicted_lon AS lon, predicted_visit_time AS timestamp,
           ST_AsBinary(geom) AS geom
    FROM view_top_daily_poi
    WHERE client_id = %s
    ORDER BY predicted_visit_time;
    """
    result = load_from_db(query, (client_id,))
    if not result:
        return gpd.GeoDataFrame(columns=["lat", "lon", "timestamp", "geom"])
    df = pd.DataFrame(result)
    df["geom"] = df["geom"].apply(lambda x: wkb.loads(x) if x else None)
    return gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")


def mapf_route_exists(client_id, lat, lon):
    query = """
    SELECT 1 FROM mapf_routes
    WHERE client_id = %s
      AND destination_lat = %s AND destination_lon = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, lat, lon))
    return bool(result)


def load_astar_path(client_id, lat, lon):
    query = """
    SELECT ST_AsText(path) AS path
    FROM astar_routes
    WHERE client_id = %s
      AND destination_lat = %s AND destination_lon = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, lat, lon))
    return wkt.loads(result[0]['path']) if result else None


def load_mapf_path(client_id, lat, lon):
    query = """
    SELECT ST_AsText(path) AS path
    FROM mapf_routes
    WHERE client_id = %s
      AND destination_lat = %s AND destination_lon = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, lat, lon))
    return wkt.loads(result[0]['path']) if result else None


def fetch_best_combined_poi(client_id):
    query = """
    SELECT
        lat,
        lon,
        ST_AsBinary(geom) AS geom
    FROM view_combined_pois
    WHERE client_id = %s
    ORDER BY
        CASE
            WHEN poi_type LIKE 'predicted_%%' THEN 1
            ELSE 0
        END DESC,
        poi_rank DESC,
        COALESCE(predicted_visit_time, NOW()) DESC NULLS LAST,
        created_at DESC
    LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    if not result:
        logging.warning(f"⚠ No combined POI found for {client_id}")
        return None

    row = result[0]
    return {
        "lat": row["lat"],
        "lon": row["lon"],
        "geom": wkb.loads(row["geom"]) if row.get("geom") else None,
    }


def save_optimized_route(client_id, stop_id, destination_coords, path,
                         segment_type='unknown', is_chosen=True, origin_coords=None):
    query = """
    INSERT INTO optimized_routes (
        client_id, stop_id, destination_lat, destination_lon,
        path, segment_type, is_chosen,
        origin_lat, origin_lon,
        created_at
    )
    VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, NOW())
    ON CONFLICT (client_id, stop_id, segment_type)
    DO UPDATE SET
        path = EXCLUDED.path,
        is_chosen = EXCLUDED.is_chosen,
        origin_lat = EXCLUDED.origin_lat,
        origin_lon = EXCLUDED.origin_lon,
        created_at = NOW();
    """
    origin_lat, origin_lon = origin_coords if origin_coords else (None, None)

    save_to_db(query, (
        client_id,
        stop_id,
        destination_coords[0],
        destination_coords[1],
        path.wkt,
        segment_type,
        is_chosen,
        origin_lat,
        origin_lon
    ))


def get_latest_speed(client_id):
    q = """
    SELECT speed
    FROM geodata
    WHERE client_id = %s
    ORDER BY updated_at DESC
    LIMIT 1;
    """
    res = load_from_db(q, (client_id,))
    if res and res[0].get("speed") is not None:
        try:
            s = float(res[0]["speed"])
            return max(s, 0.0)
        except Exception:
            pass
    return 0.0


def get_route_usage_ratios(client_id):
    """
    Returns (astar_ratio, mapf_ratio) based on how often the client ended up with
    A* (direct) vs MAPF (multimodal) in optimized_routes historically.
    Defaults to (0.5, 0.5) if no history.
    """
    q = """
    SELECT segment_type, COUNT(*) AS n
    FROM optimized_routes
    WHERE client_id = %s
    GROUP BY segment_type;
    """
    res = load_from_db(q, (client_id,))
    if not res:
        return 0.5, 0.5

    total = 0
    astar_n = 0
    mapf_n = 0
    for r in res:
        n = int(r["n"])
        total += n
        st = (r["segment_type"] or "").lower()
        if st == "direct":
            astar_n += n
        elif st == "multimodal":
            mapf_n += n

    if total == 0:
        return 0.5, 0.5
    return astar_n / total, mapf_n / total


def load_lstm_model():
    global _lstm_model, _lstm_loaded
    if _lstm_loaded and _lstm_model is not None:
        return _lstm_model

    try:
        model = keras.models.load_model(MODEL_PATH)
        # optional weights file (won't error if not present)
        try:
            model.load_weights(MODEL_WEIGHTS)
        except Exception:
            pass
        _lstm_model = model
        _lstm_loaded = True
        logging.info("✅ LSTM model loaded in routing module.")
    except Exception as e:
        logging.warning(f"⚠️ Could not load LSTM model: {e}")
        _lstm_model = None
        _lstm_loaded = True

    return _lstm_model


def has_departure_candidate(client_id, stop_id):
    """
    True if there is at least one GTFS-RT departure that lines up with A* ETA
    for this client & stop_id (per view_departure_candidates).
    """
    query = """
    SELECT 1
    FROM view_departure_candidates
    WHERE client_id = %s AND stop_id = %s
    ORDER BY departure_time
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, stop_id))
    return bool(result)


def fetch_best_departure_candidate(client_id, stop_id):
    """
    Optionally fetch the earliest matching candidate (if you want to enrich logs).
    """
    query = """
    SELECT *
    FROM view_departure_candidates
    WHERE client_id = %s AND stop_id = %s
    ORDER BY departure_time
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, stop_id))
    return result[0] if result else None


def save_reroute(client_id, stop_id, destination_coords, path_wkt,
                 segment_type, reason, origin_coords=None,
                 previous_stop_id=None, previous_segment_type=None):
    q = """
    INSERT INTO reroutes (
      client_id, stop_id, origin_lat, origin_lon,
      destination_lat, destination_lon, path,
      segment_type, reason, previous_stop_id, previous_segment_type, is_chosen, created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326),
            %s, %s, %s, %s, TRUE, NOW());
    """
    o_lat, o_lon = origin_coords if origin_coords else (None, None)
    d_lat, d_lon = destination_coords
    save_to_db(q, (client_id, stop_id, o_lat, o_lon, d_lat, d_lon, path_wkt,
                   segment_type, reason, previous_stop_id, previous_segment_type))
