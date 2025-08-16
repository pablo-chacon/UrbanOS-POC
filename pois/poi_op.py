import logging
import numpy as np
import pandas as pd
from datetime import datetime
from shapely.geometry import Point
from db.db_connection import (
    save_pois_to_db,
    load_client_trajectories,
    update_poi_arrival,
)


logging.basicConfig(level=logging.INFO)


def detect_pois(client_data, min_time=590, speed_threshold=1.0):
    """
    Detect POIs and produce per-visit records:
    - visit_start: timestamp of the point that started the dwell/slow segment
    - visit_end:   visit_start + time_spent (best-effort from your per-point dwell calc)
    """
    if not isinstance(client_data, pd.DataFrame):
        raise TypeError("Input to detect_pois must be a pandas DataFrame")

    client_data['timestamp'] = pd.to_datetime(client_data['timestamp'], errors='coerce')
    client_data = client_data.dropna(subset=['timestamp'])
    client_data = client_data.sort_values(['session_id', 'timestamp'])

    # next_time within session
    client_data['next_time'] = client_data.groupby('session_id')['timestamp'].shift(-1)
    client_data['time_spent'] = (client_data['next_time'] - client_data['timestamp']).dt.total_seconds().fillna(0)

    # Candidate visits: dwell long enough OR moving slow
    candidates = client_data[
        (client_data['time_spent'] > min_time) | (client_data['speed'] < speed_threshold)
        ].copy()

    if candidates.empty:
        logging.info("⚠ No POI candidates found.")
        return pd.DataFrame(columns=[
            'client_id', 'lat', 'lon', 'time_spent', 'poi_rank', 'poi_created_at',
            'visit_start', 'visit_end', 'duration_seconds'
        ])

    # Treat each candidate row as a visit event (you can later coalesce by proximity if you want)
    candidates['poi_rank'] = candidates.groupby(['lat', 'lon'])['time_spent'].transform('sum')
    candidates['client_id'] = client_data['client_id'].iloc[0]

    # per-visit timing
    candidates['visit_start'] = candidates['timestamp']
    candidates['visit_end'] = candidates['timestamp'] + pd.to_timedelta(candidates['time_spent'], unit='s')
    candidates['duration_seconds'] = candidates['time_spent'].astype('int64', errors='ignore')
    candidates['poi_created_at'] = candidates['timestamp']

    cols = [
        'client_id', 'lat', 'lon', 'time_spent', 'poi_rank', 'poi_created_at',
        'visit_start', 'visit_end', 'duration_seconds'
    ]
    out = candidates[cols].drop_duplicates()
    logging.info(f"✅ POI detection finished: {len(out)} visits for client_id {out['client_id'].iloc[0]}")
    return out


def process_client_pois(client_id):
    client_data = load_client_trajectories(client_id)
    if client_data.empty:
        logging.info(f"⚠ No trajectory data available for client_id: {client_id} — skipping POI detection.")
        return

    pois_df = detect_pois(client_data)
    if pois_df.empty:
        logging.warning(f"⚠ No POIs detected for client_id: {client_id}")
        return

    # Existing behavior: persist detected POIs
    save_pois_to_db(pois_df)

    # NEW: record an arrival for each visit (dedupe by lat, lon, visit_start)
    arrivals = pois_df[['lat', 'lon', 'visit_start']].drop_duplicates()
    for _, r in arrivals.iterrows():
        update_poi_arrival(client_id, float(r.lat), float(r.lon), r.visit_start)

    logging.info(f"✅ Saved {len(pois_df)} POIs + recorded {len(arrivals)} arrivals for client_id: {client_id}")
