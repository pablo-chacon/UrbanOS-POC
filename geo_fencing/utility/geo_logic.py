import logging
from shapely.geometry import Point, MultiPoint
import geopandas as gpd


def compute_geo_fence(trajectory_points):
    """
    Generate an elastic geo-fence polygon from trajectory points.
    - If only one point: use a small buffer (~100m).
    - If two points: use envelope.
    - Otherwise: use convex hull.

    Returns WKT string or None.
    """
    try:
        if not trajectory_points or len(trajectory_points) == 0:
            logging.warning("⚠ No trajectory points received for geo-fence generation.")
            return None

        coords = [
            (pt["lon"], pt["lat"])
            for pt in trajectory_points
            if pt.get("lat") is not None and pt.get("lon") is not None
        ]

        if not coords:
            logging.warning("⚠ No valid coordinates extracted from trajectory points.")
            return None

        gdf = gpd.GeoDataFrame(geometry=[Point(lon, lat) for lon, lat in coords], crs="EPSG:4326")

        if len(gdf) == 1:
            # One point: buffer ~100m (0.001° ≈ ~100m latitude)
            polygon = gdf.geometry.iloc[0].buffer(0.001)
        elif len(gdf) == 2:
            # Two points: simple envelope
            polygon = MultiPoint(gdf.geometry).envelope
        else:
            # Three or more: convex hull
            polygon = gdf.unary_union.convex_hull

        return polygon.wkt

    except Exception as e:
        logging.error(f"❌ Exception while generating geo-fence: {e}", exc_info=True)
        return None
