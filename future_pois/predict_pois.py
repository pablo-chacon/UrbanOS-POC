import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from shapely.geometry import Point
from sklearn.cluster import DBSCAN
from db.db_connection import get_db_connection, save_to_db

DAY_DECAY = 1 / (24 * 3600)
WEEK_DECAY = 1 / (7 * 24 * 3600)


def get_poi_and_patterns(client_id):
    conn = get_db_connection()

    pois_q = """
        SELECT lat, lon, time_spent, poi_rank, created_at 
        FROM pois 
        WHERE client_id = %s 
        ORDER BY created_at DESC
    """
    patterns_q = """
        SELECT lat, lon, pattern_type, timestamp 
        FROM user_patterns 
        WHERE client_id = %s 
        ORDER BY timestamp DESC
    """

    pois_df, patterns_df = pd.DataFrame(), pd.DataFrame()

    with conn.cursor() as cur:
        cur.execute(pois_q, (client_id,))
        pois = cur.fetchall()
        if pois:
            pois_df = pd.DataFrame(pois, columns=["lat", "lon", "time_spent", "poi_rank", "created_at"])

        cur.execute(patterns_q, (client_id,))
        patterns = cur.fetchall()
        if patterns:
            patterns_df = pd.DataFrame(patterns, columns=["lat", "lon", "pattern_type", "timestamp"])

    return pois_df, patterns_df


def predict_next_poi(client_id, prediction_type, fetch_data_fn):
    pois_df, patterns_df = fetch_data_fn(client_id)
    if pois_df.empty:
        logging.warning(f"⚠ No POIs for {client_id}")
        return None

    now = datetime.utcnow()
    decay_factor = DAY_DECAY if prediction_type == "daily" else WEEK_DECAY

    pois_df["created_at"] = pd.to_datetime(pois_df["created_at"])
    pois_df["time_weight"] = np.log(pois_df["time_spent"] + 1)
    pois_df["time_decay"] = np.exp(-(now - pois_df["created_at"]).dt.total_seconds() * decay_factor)

    pois_df["pattern_weight"] = 1.0
    for _, row in patterns_df.iterrows():
        match = (abs(pois_df["lat"] - row["lat"]) < 0.002) & (abs(pois_df["lon"] - row["lon"]) < 0.002)
        pois_df.loc[match, "pattern_weight"] += 1.5

    pois_df["poi_score"] = pois_df["poi_rank"] * pois_df["time_weight"] * pois_df["time_decay"] * pois_df[
        "pattern_weight"]
    pois_df = pois_df.sort_values("poi_score", ascending=False)

    visit_sequence, estimated_time = [], now
    avg_time = pois_df["time_spent"].median() if not pois_df["time_spent"].isna().all() else 1800

    for _, poi in pois_df.iterrows():
        visit_sequence.append({
            "lat": poi["lat"],
            "lon": poi["lon"],
            "predicted_visit_time": estimated_time
        })
        estimated_time += timedelta(seconds=avg_time)

    store_predicted_poi_sequence(client_id, visit_sequence, prediction_type)
    return visit_sequence


def store_predicted_poi_sequence(client_id, visit_sequence, prediction_type):
    if not visit_sequence:
        logging.warning(f"⚠ No sequence to store for {client_id}")
        return

    try:
        coords = np.array([[poi["lat"], poi["lon"]] for poi in visit_sequence])
        db = DBSCAN(eps=0.0015, min_samples=1).fit(coords)

        now = pd.Timestamp.utcnow()
        labels = db.labels_

        for label in set(labels):
            group = coords[labels == label]
            center = group.mean(axis=0)
            visit_time = visit_sequence[labels.tolist().index(label)]["predicted_visit_time"]

            row = {
                "client_id": client_id,
                "predicted_lat": float(center[0]),
                "predicted_lon": float(center[1]),
                "predicted_visit_time": visit_time,
                "prediction_type": prediction_type,
                "geom": {"lat": float(center[0]), "lon": float(center[1])},
                "created_at": now,
            }

            save_to_db("predicted_pois_sequence", row)

        logging.info(f"✅ Stored {len(set(labels))} clustered predictions for {client_id}")
    except Exception as e:
        logging.error(f"❌ Clustering failed for {client_id}: {e}")
