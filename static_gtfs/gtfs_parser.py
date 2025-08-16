import os
import requests
import zipfile
import io
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from email.utils import parsedate_to_datetime

load_dotenv()

GTFS_URL = os.getenv("GTFS_URL").format(
    operator=os.getenv("GTFS_OPERATOR"),
    apikey=os.getenv("GTFS_STATIC_KEY")
)

GTFS_TABLE_MAPPING = {
    "stops.txt": "gtfs_stops",
    "routes.txt": "gtfs_routes",
    "trips.txt": "gtfs_trips",
    "stop_times.txt": "gtfs_stop_times",
    "calendar.txt": "gtfs_calendar",
    "calendar_dates.txt": "gtfs_calendar_dates"
}

GTFS_METADATA_FILE = "last_modified.txt"  # Store timestamp of last update


def load_static_gtfs():
    headers = {
        "Accept-Encoding": "gzip"
    }

    # ✅ Load previous timestamp
    if os.path.exists(GTFS_METADATA_FILE):
        with open(GTFS_METADATA_FILE, "r") as f:
            last_modified = f.read().strip()
            if last_modified:
                headers["If-Modified-Since"] = last_modified

    response = requests.get(GTFS_URL, headers=headers)

    if response.status_code == 304:
        print("🔁 GTFS zip not modified since last fetch. Skipping download.")
        return None  # Or return previously parsed data if cached

    if response.status_code != 200:
        raise Exception(f"Failed to fetch GTFS zip. HTTP {response.status_code}")

    # ✅ Save new Last-Modified timestamp
    if "Last-Modified" in response.headers:
        with open(GTFS_METADATA_FILE, "w") as f:
            f.write(response.headers["Last-Modified"])

    zip_data = zipfile.ZipFile(io.BytesIO(response.content))
    parsed = {}

    for file_name, table_name in GTFS_TABLE_MAPPING.items():
        if file_name not in zip_data.namelist():
            print(f"⚠️ Missing {file_name} in GTFS zip")
            continue

        print(f"📄 Parsing {file_name}")
        df = pd.read_csv(zip_data.open(file_name))
        parsed[table_name] = df

    return parsed


# Parser Functions Per Table


def parse_stops(df):
    required_cols = [
        "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon",
        "zone_id", "stop_url", "location_type", "parent_station",
        "stop_timezone", "wheelchair_boarding", "platform_code"
    ]
    df = df.reindex(columns=required_cols)

    df["stop_id"] = df["stop_id"].astype(str)
    df["parent_station"] = df["parent_station"].astype(str).where(pd.notnull(df["parent_station"]), None)

    df["stop_lat"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon"] = pd.to_numeric(df["stop_lon"], errors="coerce")

    for col in ["location_type", "wheelchair_boarding"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")
        df[col] = df[col].clip(lower=0, upper=255)

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def parse_routes(df):
    required_cols = [
        "route_id", "agency_id", "route_short_name",
        "route_long_name", "route_type", "route_desc"
    ]
    df = df.reindex(columns=required_cols)
    df["route_id"] = df["route_id"].astype(str)
    df["agency_id"] = df["agency_id"].astype(str).where(pd.notnull(df["agency_id"]), None)
    df["route_type"] = pd.to_numeric(df["route_type"], errors="coerce").fillna(0).astype("Int64")
    df["route_type"] = df["route_type"].clip(lower=0, upper=255)

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def parse_trips(df):
    required_cols = [
        "trip_id", "route_id", "service_id",
        "trip_headsign", "direction_id", "shape_id"
    ]
    df = df.reindex(columns=required_cols)
    df["trip_id"] = df["trip_id"].astype(str)
    df["route_id"] = df["route_id"].astype(str)
    df["service_id"] = df["service_id"].astype(str)
    df["direction_id"] = pd.to_numeric(df["direction_id"], errors="coerce").fillna(0).astype("Int64")
    df["direction_id"] = df["direction_id"].clip(lower=0, upper=1)

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def parse_stop_times(df):
    required_cols = [
        "trip_id", "arrival_time", "departure_time", "stop_id",
        "stop_sequence", "stop_headsign", "pickup_type",
        "drop_off_type", "shape_dist_traveled", "timepoint",
        "pickup_booking_rule_id", "drop_off_booking_rule_id"
    ]
    df = df.reindex(columns=required_cols)
    df["trip_id"] = df["trip_id"].astype(str)
    df["stop_id"] = df["stop_id"].astype(str)

    for col in ["stop_sequence", "pickup_type", "drop_off_type", "timepoint"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")
        df[col] = df[col].clip(lower=0, upper=32767)

    df["shape_dist_traveled"] = pd.to_numeric(df["shape_dist_traveled"], errors="coerce")

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def parse_calendar(df):
    required_cols = [
        "service_id", "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "end_date"
    ]
    df = df.reindex(columns=required_cols)
    df["service_id"] = df["service_id"].astype(str)

    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for day in weekdays:
        df[day] = pd.to_numeric(df[day], errors="coerce").fillna(0).astype("Int64")
        df[day] = df[day].clip(lower=0, upper=1)

    df["start_date"] = pd.to_datetime(df["start_date"], format="%Y%m%d", errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def parse_calendar_dates(df):
    required_cols = ["service_id", "date", "exception_type"]
    df = df.reindex(columns=required_cols)
    df["service_id"] = df["service_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df["exception_type"] = pd.to_numeric(df["exception_type"], errors="coerce").fillna(0).astype("Int64")
    df["exception_type"] = df["exception_type"].clip(lower=1, upper=2)

    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")
