import json
import paho.mqtt.client as mqtt
import logging
from db.db_connection import insert_data, get_db_connection

logging.basicConfig(level=logging.INFO)

# MQTT Configuration
MQTT_BROKER = "mqtt-broker"
MQTT_PORT = 1883
MQTT_TOPIC = "client/+/session/+/"


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("✅ Connected to MQTT Broker.")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(f"❌ Connection failed with code {rc}")


def on_message(client, userdata, msg):
    try:
        topic_parts = msg.topic.split("/")
        if len(topic_parts) < 3:
            logging.warning(f"⚠ Invalid topic format: {msg.topic}")
            return

        client_id = str(topic_parts[1])
        payload = json.loads(msg.payload.decode("utf-8"))
        logging.info(f"📦 Received payload for client {client_id}:\n{json.dumps(payload, indent=2)}")

        start_time = payload.get("start_time")
        end_time = payload.get("end_time")
        trajectory = payload.get("trajectory", [])

        if not start_time or not end_time:
            logging.warning(f"⚠ Missing session window in payload. Skipping.")
            return

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mqtt_sessions (client_id, start_time, end_time)
                VALUES (%s, %s, %s)
                ON CONFLICT (client_id, start_time) DO NOTHING
                RETURNING session_id;
                """,
                (client_id, start_time, end_time)
            )
            result = cur.fetchone()

            # If session already existed, fetch the existing session_id
            if not result:
                cur.execute(
                    """
                    SELECT session_id
                    FROM mqtt_sessions
                    WHERE client_id = %s AND start_time = %s
                    """,
                    (client_id, start_time)
                )
                result = cur.fetchone()

            if not result:
                logging.error("❌ Failed to retrieve or create session_id.")
                return

            session_id = result["session_id"]
            logging.info(f"🆔 Using session_id {session_id} for client {client_id}")

        trajectory_data = [
            (
                session_id,
                client_id,
                point["lat"],
                point["lon"],
                point.get("elevation"),
                point.get("speed"),
                point.get("activity"),
                point["timestamp"],
                f"SRID=4326;POINT({point['lon']} {point['lat']})"
            )
            for point in trajectory
            if is_valid_point(point)
        ]

        if trajectory_data:
            insert_data(trajectory_data=trajectory_data)
            logging.info(f"✅ Inserted {len(trajectory_data)} geodata points for session {session_id}.")

    except ValueError as ve:
        logging.error(f"❌ Value error: {ve}")
    except json.JSONDecodeError:
        logging.error("❌ JSON decoding failed.")
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}", exc_info=True)


def is_valid_point(point):
    return (
            point.get("lat") is not None and
            point.get("lon") is not None and
            point.get("timestamp") not in (None, "", "null")
    )


def start_mqtt_subscriber():
    conn = get_db_connection()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=conn)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    start_mqtt_subscriber()
