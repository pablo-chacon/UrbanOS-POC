import requests
from google.transit import gtfs_realtime_pb2
from .gtfs_parsers import (
    parse_vehicle_positions,
    parse_trip_updates,
    parse_service_alerts
)

# --- URL router ---
PARSERS = {
    "vehicle_positions": parse_vehicle_positions,
    "trip_updates": parse_trip_updates,
    "service_alerts": parse_service_alerts,
}


def fetch_gtfs_realtime(url: str, data_type: str):
    """
    Generic GTFS-RT fetcher and parser.

    Args:
        url (str): Full GTFS-RT URL
        data_type (str): One of ["vehicle_positions", "trip_updates", "service_alerts"]

    Returns:
        list[dict]: Parsed GTFS-RT rows
    """
    if data_type not in PARSERS:
        raise ValueError(f"Unsupported data_type: {data_type}")

    response = requests.get(url)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    return PARSERS[data_type](feed)
