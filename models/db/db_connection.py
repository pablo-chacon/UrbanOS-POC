import os
import logging
import time
import psycopg
import pandas as pd
import geopandas as gpd
from psycopg.rows import dict_row
from shapely import wkb
from dotenv import load_dotenv

load_dotenv()

# ✅ Ensure default PostgreSQL port if missing
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# ✅ Global database connection
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
                port=POSTGRES_PORT,
                autocommit=True,
                row_factory=dict_row
            )
            logging.info("✅ Successfully connected to the database.")
            return db_connection
        except psycopg.OperationalError as e:
            logging.warning(f"⚠️ Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to the database after multiple attempts")


def load_from_db(query, conditions=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, conditions if conditions else ())
        result = cursor.fetchall()

    df = pd.DataFrame(result)

    if df.empty:
        return gpd.GeoDataFrame()

    if "geom" in df.columns:
        # Expecting ST_AsBinary(geom) alias -> bytes
        df["geom"] = df["geom"].apply(lambda x: wkb.loads(x) if x else None)
        return gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")

    return df


def fetch_pois(client_id):
    query = """
    SELECT p.lat, p.lon, ST_AsBinary(p.geom) AS geom
    FROM pois p
    WHERE p.client_id = %s;
    """
    return load_from_db(query, (str(client_id),))


def fetch_stop_points():
    query = """
    SELECT stop_name, stop_lat, stop_lon, ST_AsBinary(geom) AS geom
    FROM gtfs_stops;
    """
    return load_from_db(query)


def load_vehicle_arrivals_for_training():
    """
    Fetch timestamped vehicle positions as training points.
    Keep a real 'timestamp' for temporal ordering.
    """
    query = """
    SELECT position_lat AS lat, position_lon AS lon, created_at
    FROM vehicle_arrivals
    WHERE position_lat IS NOT NULL AND position_lon IS NOT NULL;
    """
    df = load_from_db(query)

    if df.empty:
        return pd.DataFrame()

    # hour in [0,1]
    df["hour"] = pd.to_datetime(df["created_at"], errors="coerce").dt.hour / 23.0
    # keep timestamp for ordering
    df.rename(columns={"created_at": "timestamp"}, inplace=True)

    # placeholders
    df["speed"] = 0.0
    df["elevation"] = 0.0
    df["act_walk"] = 0
    df["act_vehicle"] = 0
    df["act_stationary"] = 0
    df["act_unknown"] = 1

    return df[["lat", "lon", "hour", "speed", "elevation",
               "act_walk", "act_vehicle", "act_stationary", "act_unknown", "timestamp"]]


def load_astar_mapf_vectors():
    """
    Load origin-destination coordinates from astar_routes and mapf_routes tables.
    These reflect historically chosen routes (client decisions).
    Preserve created_at as 'timestamp' for ordering.
    """
    query = """
    SELECT origin_lat AS lat, origin_lon AS lon, created_at
    FROM astar_routes
    WHERE origin_lat IS NOT NULL AND origin_lon IS NOT NULL

    UNION

    SELECT destination_lat AS lat, destination_lon AS lon, created_at
    FROM mapf_routes
    WHERE destination_lat IS NOT NULL AND destination_lon IS NOT NULL;
    """
    df = load_from_db(query)

    if df.empty:
        return pd.DataFrame()

    df["hour"] = pd.to_datetime(df["created_at"], errors="coerce").dt.hour / 23.0
    df.rename(columns={"created_at": "timestamp"}, inplace=True)

    # placeholders
    df["speed"] = 0.0
    df["elevation"] = 0.0
    df["act_walk"] = 0
    df["act_vehicle"] = 0
    df["act_stationary"] = 0
    df["act_unknown"] = 1

    return df[["lat", "lon", "hour", "speed", "elevation",
               "act_walk", "act_vehicle", "act_stationary", "act_unknown", "timestamp"]]


def load_full_trajectory_points():
    """
    Explodes all JSONB points from `trajectories.trajectory` into a flattened DataFrame.
    """
    query = """
    SELECT jsonb_array_elements(trajectory)::jsonb AS point
    FROM trajectories
    WHERE trajectory IS NOT NULL;
    """
    rows = load_from_db(query)
    if rows.empty:
        return pd.DataFrame()

    points = []
    for row in rows.itertuples(index=False):
        p = row.point
        try:
            points.append({
                "lat": float(p["lat"]),
                "lon": float(p["lon"]),
                "speed": float(p.get("speed", 0.0)),
                "elevation": float(p.get("elevation", 0.0)),
                "activity": p.get("activity"),
                "timestamp": p.get("timestamp")
            })
        except Exception:
            continue

    df = pd.DataFrame(points)
    if df.empty:
        return df

    # hour in [0,1]
    df["hour"] = pd.to_datetime(df.get("timestamp"), errors="coerce").dt.hour.fillna(0) / 23.0

    # activity one-hot
    def _act(a):
        a = (a if isinstance(a, str) else "").strip().lower()
        if a in {"walk", "walking", "foot"}:
            return (1, 0, 0, 0)
        if a in {"vehicle", "bus", "tram", "train", "car", "bike"}:
            return (0, 1, 0, 0)
        if a in {"stationary", "idle", "stop"}:
            return (0, 0, 1, 0)
        return (0, 0, 0, 1)

    ohe = df["activity"].apply(_act)
    df[["act_walk", "act_vehicle", "act_stationary", "act_unknown"]] = pd.DataFrame(ohe.tolist(), index=df.index)

    return df[["lat", "lon", "hour", "speed", "elevation",
               "act_walk", "act_vehicle", "act_stationary", "act_unknown", "timestamp"]]


def load_training_vectors():
    """
    Build aggregated training vectors from:
      - user_patterns, pois
      - astar/mapf (origins + destinations)
      - GTFS static (stops)
      - vehicle_arrivals (GTFS-RT derived)
      - full raw trajectory points (immutable ground truth)

    Output columns (with time kept for ordering):
      ['lat','lon','hour','speed','elevation','act_walk','act_vehicle','act_stationary','act_unknown','timestamp']
    """
    queries = {
        "patterns": """
            SELECT lat, lon, timestamp
            FROM user_patterns
            WHERE lat IS NOT NULL AND lon IS NOT NULL;
        """,
        "pois": """
            SELECT p.lat, p.lon, p.visit_start AS timestamp
            FROM pois p
            WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL;
        """,
        "pois_sequence": """
            SELECT predicted_lat AS lat, predicted_lon AS lon, predicted_visit_time AS timestamp
            FROM predicted_pois_sequence
            WHERE predicted_lat IS NOT NULL AND predicted_lon IS NOT NULL;
        """,
        "astar_mapf": """
            SELECT origin_lat AS lat, origin_lon AS lon, created_at AS timestamp
            FROM astar_routes
            WHERE origin_lat IS NOT NULL AND origin_lon IS NOT NULL
            UNION
            SELECT destination_lat AS lat, destination_lon AS lon, created_at AS timestamp
            FROM mapf_routes
            WHERE destination_lat IS NOT NULL AND destination_lon IS NOT NULL;
        """,
        "gtfs": """
            SELECT stop_lat AS lat, stop_lon AS lon, NULL::timestamp AS timestamp
            FROM view_static_gtfs_unified
            WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL;
        """
    }

    dfs = []

    # 1) Tabular sources (patterns/pois/astar+mapf/gtfs)
    for name, query in queries.items():
        df = load_from_db(query)
        if df.empty:
            continue

        # harmonize schema
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])

        # hour in [0,1]
        df["hour"] = pd.to_datetime(df.get("timestamp"), errors="coerce").dt.hour.fillna(0) / 23.0

        # placeholders for non-trajectory sources
        df["speed"] = 0.0
        df["elevation"] = 0.0

        # activity one-hot (unknown for these sources)
        df["act_walk"] = 0
        df["act_vehicle"] = 0
        df["act_stationary"] = 0
        df["act_unknown"] = 1

        dfs.append(df[["lat", "lon", "hour", "speed", "elevation",
                       "act_walk", "act_vehicle", "act_stationary", "act_unknown", "timestamp"]])

    # 2) Vehicle arrivals (real-world network behavior)
    df_arr = load_vehicle_arrivals_for_training()
    if not df_arr.empty:
        dfs.append(df_arr)

    # 3) Full raw trajectories (immutable ground truth)
    df_traj = load_full_trajectory_points()
    if not df_traj.empty:
        # coerce
        for col in ["lat", "lon", "speed", "elevation"]:
            df_traj[col] = pd.to_numeric(df_traj.get(col), errors="coerce")
        df_traj = df_traj.dropna(subset=["lat", "lon"])
        dfs.append(df_traj)

    if not dfs:
        logging.warning("⚠ No training vectors available.")
        return pd.DataFrame()

    # Concatenate
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.dropna(subset=["lat", "lon"])

    # ✅ Keep temporal order for LSTM
    df_all["timestamp"] = pd.to_datetime(df_all.get("timestamp"), errors="coerce")
    before = len(df_all)
    df_all = df_all.dropna(subset=["timestamp"]).sort_values("timestamp")
    after = len(df_all)
    logging.info(f"🕒 Kept {after}/{before} rows with valid timestamp for temporal training.")

    # Light dedup to keep memory in check (preserve order)
    df_all = df_all.drop_duplicates(subset=["lat", "lon", "hour"], keep="first")

    logging.info(
        f"✅ Aggregated training vector count: {len(df_all)} "
        f"(including full trajectories = {len(df_traj) if 'df_traj' in locals() and not df_traj.empty else 0})"
    )

    return df_all
