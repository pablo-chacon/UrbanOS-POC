import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from db.db_connection import load_from_db, save_to_db
import logging
import json
from shapely.geometry import LineString

logging.basicConfig(level=logging.INFO)


def process_client_clusters(client_id, n_clusters=8):
    """
    Perform clustering on full trajectory history and save result as LineString.
    """
    query = "SELECT session_id, trajectory FROM trajectories WHERE client_id = %s"
    data = load_from_db(query, (client_id,))

    if not data:
        logging.info(f"⚠ No trajectory data found for client_id: {client_id}")
        return

    trajectory_rows = []
    for row in data:
        session_id = row["session_id"]
        trajectory = row["trajectory"]

        try:
            if isinstance(trajectory, str):
                trajectory = json.loads(trajectory)
            elif not isinstance(trajectory, list):
                logging.warning(f"⚠ Unexpected format for session {session_id}")
                continue

            for point in trajectory:
                if isinstance(point, dict) and "lat" in point and "lon" in point:
                    point["session_id"] = session_id
                    trajectory_rows.append(point)

        except (json.JSONDecodeError, TypeError) as e:
            logging.error(f"❌ Error parsing trajectory for session {session_id}: {e}")
            continue

    if not trajectory_rows:
        logging.info(f"⚠ No valid trajectory data to process for client_id: {client_id}")
        return

    df = pd.DataFrame(trajectory_rows)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)

    if df.empty:
        logging.warning(f"⚠ No usable points for clustering for client_id: {client_id}")
        return

    logging.info(f"✅ {len(df)} points loaded for clustering for client_id: {client_id}")

    # Clustering
    coords = df[["lat", "lon"]]
    scaler = StandardScaler()
    scaled = scaler.fit_transform(coords)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(scaled)

    cluster_centers = kmeans.cluster_centers_
    real_centers = scaler.inverse_transform(cluster_centers)

    logging.info(f"✅ Cluster centers for {client_id}: {real_centers}")

    for idx in sorted(df["cluster"].unique()):
        cluster_df = df[df["cluster"] == idx].sort_values("timestamp")
        if len(cluster_df) < 2:
            continue  # skip single-point clusters

        line = LineString(zip(cluster_df["lon"], cluster_df["lat"]))

        save_to_db("user_patterns", {
            "client_id": client_id,
            "lat": line.centroid.y,
            "lon": line.centroid.x,
            "pattern_type": f"Cluster {idx + 1}",
            "geom": line.wkt
        })

    logging.info(f"✅ Clustering results saved for client_id: {client_id}")
