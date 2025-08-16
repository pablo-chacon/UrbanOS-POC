# hotspot_detection.py
from shapely.geometry import Point
from geoalchemy2.elements import WKTElement
from sklearn.cluster import DBSCAN
import numpy as np


def expand_trajectories(rows, source_type="trajectory"):
    expanded = []
    for row in rows:
        client_id = row["client_id"]
        trajectory = row.get("trajectory", [])
        for point in trajectory:
            if "lat" in point and "lon" in point:
                expanded.append({
                    "client_id": client_id,
                    "lat": point["lat"],
                    "lon": point["lon"],
                    "source_type": source_type
                })
    return expanded


def detect_hotspots(points, eps=0.005, min_samples=5):
    """
    Detect hotspots using DBSCAN patterns on geospatial points, grouped by client_id.
    :param points: List of input points with lat, lon, client_id, source_type
    """
    grouped = {}
    for p in points:
        grouped.setdefault(p["client_id"], []).append(p)

    hotspots = []
    for client_id, items in grouped.items():
        coords = np.array([[p['lat'], p['lon']] for p in items])
        db = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(np.radians(coords))
        labels = db.labels_

        for label in set(labels):
            if label == -1:
                continue

            cluster_points = coords[labels == label]
            cluster_items = [items[i] for i in range(len(labels)) if labels[i] == label]
            centroid_lat = np.mean(cluster_points[:, 0])
            centroid_lon = np.mean(cluster_points[:, 1])
            radius = max(np.linalg.norm(cluster_points - [centroid_lat, centroid_lon], axis=1)) * 111_000

            hotspots.append({
                "client_id": client_id,
                "lat": centroid_lat,
                "lon": centroid_lon,
                "radius": radius,
                "density": len(cluster_points),
                "type": "hotspot",
                "source_type": cluster_items[0].get("source_type", "trajectory"),
                "geom": WKTElement(f"POINT({centroid_lon} {centroid_lat})", srid=4326)
            })

    return hotspots