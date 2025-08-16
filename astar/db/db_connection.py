import os
import time
import logging
import psycopg
import pandas as pd
import geopandas as gpd
from shapely.wkb import loads
from shapely.geometry import LineString
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()
db_connection = None


def get_db_connection(retries=5, delay=5):
    global db_connection
    if db_connection is not None and not db_connection.closed:
        return db_connection

    for attempt in range(retries):
        try:
            db_connection = psycopg.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠ Attempt {attempt + 1}: {e}")
            time.sleep(delay)

    raise Exception("❌ Could not connect to the database after multiple attempts.")


def load_from_db(query, params=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ Error loading data: {e}")
        return None


def fetch_latest_locations():
    """
    Return a list of tuples (client_id, lat, lon, updated_at) for clients
    with a location updated at least 2 seconds ago.
    """
    query = """
    SELECT DISTINCT ON (client_id) client_id, lat, lon, speed, updated_at
    FROM geodata
    WHERE updated_at <= NOW() - INTERVAL '2 seconds'
      AND timestamp <= NOW() + INTERVAL '5 minutes'
    ORDER BY client_id, updated_at DESC
    """
    results = load_from_db(query)
    if not results:
        logging.warning("⚠ No recent geodata found for any client.")
        return []
    return [(row["client_id"], row["lat"], row["lon"], row["speed"], row["updated_at"]) for row in results]


def fetch_next_predicted_poi(client_id):
    query = """
        SELECT
            lat,
            lon,
            predicted_visit_time AS timestamp,
            ST_AsBinary(geom) AS geom
        FROM view_combined_pois
        WHERE client_id = %s
        ORDER BY
            CASE
                WHEN poi_type LIKE 'predicted_%%' THEN 1  -- predicted first
                ELSE 0
            END DESC,
            poi_rank DESC,
            COALESCE(predicted_visit_time, NOW()) DESC NULLS LAST,
            created_at DESC
        LIMIT 1;
    """
    result = load_from_db(query, (client_id,))
    if not result:
        return gpd.GeoDataFrame(columns=["lat", "lon", "timestamp", "geom"])

    df = pd.DataFrame(result)
    df["geom"] = df["geom"].apply(lambda x: loads(x) if x else None)
    return gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")


def fetch_fallback_stop_point(current_lat, current_lon):
    """
    Fetch the nearest stop from gtfs_stops table.
    """
    query = """
    SELECT stop_id, stop_lat, stop_lon
    FROM gtfs_stops
    WHERE location_type = 0
    ORDER BY ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
             <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    LIMIT 1;
    """
    result = load_from_db(query, (current_lon, current_lat))
    if not result:
        logging.warning("⚠ No GTFS stops found for fallback.")
        return None
    return result[0]["stop_lat"], result[0]["stop_lon"], result[0]["stop_id"]


def fetch_existing_poi_targets(client_id, lat, lon, tolerance_meters=30):
    """
    Attempts to find a matching POI ID from the 'pois' table near the predicted location.
    Returns {'poi_id': ...} if found, else None.
    """
    query = """
    SELECT poi_id
    FROM pois
    WHERE client_id = %s
      AND ST_DWithin(
            geom,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s
      )
    ORDER BY ST_Distance(
        geom,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    )
    LIMIT 1;
    """
    result = load_from_db(query, (client_id, lon, lat, tolerance_meters, lon, lat))
    if result:
        return {"poi_id": result[0]["poi_id"]}
    return None


def fetch_closest_stop_id(destination_coords):
    """
    Find the closest GTFS stop to the given (lat, lon) and return its stop_id.
    """
    query = """
    SELECT stop_id
    FROM gtfs_stops
    WHERE location_type = 0
    ORDER BY ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
             <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    LIMIT 1;
    """
    lat, lon = destination_coords
    result = load_from_db(query, (lon, lat))

    if not result:
        logging.warning(f"⚠ No stop_id found near POI at ({lat}, {lon})")
        return None

    return result[0]["stop_id"]


def save_astar_route(client_id, stop_id, target_type, parent_station, poi_id,
                     origin_coords, destination_coords, path_gdf,
                     speed, decision_context="initial_prediction", efficiency_score=None):
    """
    Save the A* route to the database using GTFS-based identifiers.
    """
    if path_gdf.empty or not isinstance(path_gdf, pd.DataFrame):
        logging.warning("⚠ Cannot save A* route: no path data.")
        return

    coords = path_gdf.geometry.to_list()
    if len(coords) < 2:
        logging.warning(f"⚠ Not enough points to form LineString for client {client_id}. Skipping route save.")
        return

    path = LineString(coords)
    distance = path_gdf["distance"].sum()

    try:
        predicted_eta = pd.Timestamp.utcnow() + pd.to_timedelta(distance / speed, unit="s")
    except Exception as e:
        logging.error(f"❌ Failed to calculate predicted_eta for client {client_id}: {e}")
        return

    query = """
    INSERT INTO astar_routes (
        client_id, stop_id, target_type, parent_station, poi_id,
        origin_lat, origin_lon,
        destination_lat, destination_lon, path, distance,
        efficiency_score, decision_context, predicted_eta
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (
                client_id,
                stop_id,
                target_type,
                parent_station,
                poi_id,
                origin_coords[0],
                origin_coords[1],
                destination_coords[0],
                destination_coords[1],
                path.wkt,
                distance,
                efficiency_score,
                decision_context,
                predicted_eta.to_pydatetime()
            ))
            logging.info(
                f"✅ Saved A* route for {client_id} (stop_id={stop_id}, poi_id={poi_id}, ETA={predicted_eta})"
            )
    except Exception as e:
        logging.error(f"❌ Failed to save A* route for {client_id}: {e}")
