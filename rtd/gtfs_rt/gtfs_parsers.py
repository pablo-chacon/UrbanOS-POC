from datetime import datetime
import hashlib
from google.transit import gtfs_realtime_pb2


def parse_vehicle_positions(feed):
    rows = []
    now = datetime.utcnow()

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vp = entity.vehicle
        rows.append({
            "vehicle_id": vp.vehicle.id,
            "trip_id": vp.trip.trip_id,
            "route_id": vp.trip.route_id,
            "stop_id": vp.stop_id,
            "lat": vp.position.latitude,
            "lon": vp.position.longitude,
            "speed": vp.position.speed if vp.position.HasField("speed") else None,
            "bearing": vp.position.bearing if vp.position.HasField("bearing") else None,
            "timestamp": datetime.utcfromtimestamp(vp.timestamp) if vp.HasField("timestamp") else now,
            "created_at": now
        })

    return rows


def parse_trip_updates(feed):
    rows = []
    now = datetime.utcnow()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update.trip
        for stu in entity.trip_update.stop_time_update:
            status = (
                gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                    stu.schedule_relationship
                ) if hasattr(stu, "schedule_relationship") else "SCHEDULED"
            )

            rows.append({
                "trip_id": trip.trip_id,
                "stop_id": stu.stop_id,
                "arrival_time": datetime.utcfromtimestamp(stu.arrival.time) if stu.HasField("arrival") and stu.arrival.HasField("time") else None,
                "departure_time": datetime.utcfromtimestamp(stu.departure.time) if stu.HasField("departure") and stu.departure.HasField("time") else None,
                "delay_seconds": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None,
                "status": status,
                "created_at": now
            })

    return rows


def parse_service_alerts(feed):
    rows = []
    now = datetime.utcnow()

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        alert = entity.alert

        header = alert.header_text.translation[0].text if alert.header_text.translation else ""
        desc = alert.description_text.translation[0].text if alert.description_text.translation else ""

        cause = gtfs_realtime_pb2.Alert.Cause.Name(alert.cause) if hasattr(alert, "cause") else "UNKNOWN"
        effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect) if hasattr(alert, "effect") else "UNKNOWN"

        for informed in alert.informed_entity:
            affected = informed.route_id or informed.stop_id or informed.trip.trip_id or "unknown"
            unique_string = f"{header}-{desc}-{affected}-{now.isoformat()}"
            alert_id = hashlib.sha256(unique_string.encode()).hexdigest()

            rows.append({
                "alert_id": alert_id,
                "cause": cause,
                "effect": effect,
                "header_text": header,
                "description_text": desc,
                "affected_entity": affected,
                "start_time": datetime.utcfromtimestamp(alert.active_period[0].start) if alert.active_period else None,
                "end_time": datetime.utcfromtimestamp(alert.active_period[0].end) if alert.active_period else None,
                "created_at": now
            })

    return rows
