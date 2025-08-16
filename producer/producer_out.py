# producer/producer_out.py (same changes apply to producer/db/db_connection.py if that's your runner)
import os
import json
import time
import logging
from typing import Set, Tuple

import paho.mqtt.client as mqtt
from db.db_connection import fetch_optimized_route  # must return session_id now

logging.basicConfig(level=logging.INFO)

BROKER = os.getenv("MQTT_BROKER", "mqtt-broker")
PORT = int(os.getenv("MQTT_PORT", 1883))

# Default includes session; still tolerate envs without {session_id}
RESULTS_TOPIC_TEMPLATE = (os.getenv(
    "MQTT_RESULTS_TOPIC",
    "results/client/{client_id}/session/{session_id}/"
)).rstrip("/")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("✅ Connected to MQTT broker")
    else:
        logging.error(f"❌ MQTT connect failed (rc={rc})")


def on_disconnect(client, userdata, rc):
    logging.warning(f"⚠ Disconnected from MQTT (rc={rc}), retrying in 5s…")
    time.sleep(5)
    try:
        client.reconnect()
    except Exception as e:
        logging.error(f"❌ Reconnect failed: {e}")


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect(BROKER, PORT, keepalive=60)

# dedupe key includes session_id
_seen: Set[Tuple[str, int, str]] = set()


def publish_results(poll_seconds: int = 5):
    """Poll DB for fresh unified routes and publish to per-client+session topic."""
    while True:
        rows = fetch_optimized_route()
        if not rows:
            logging.info("⏳ No fresh routes to publish.")
        else:
            for r in rows:
                client_id = str(r["client_id"])
                session_id = int(r["session_id"])
                created_at_iso = r["created_at"].isoformat() if r.get("created_at") else ""
                key = (client_id, session_id, created_at_iso)

                if key in _seen:
                    continue
                _seen.add(key)

                # tolerate env templates that don't have {session_id}
                if "{session_id}" in RESULTS_TOPIC_TEMPLATE:
                    topic = RESULTS_TOPIC_TEMPLATE.format(client_id=client_id, session_id=session_id)
                else:
                    topic = RESULTS_TOPIC_TEMPLATE.format(client_id=client_id)

                payload = {
                    "client_id": client_id,
                    "session_id": session_id,
                    "stop_id": r.get("stop_id"),
                    "destination": {
                        "lat": r.get("destination_lat"),
                        "lon": r.get("destination_lon"),
                    },
                    "route_path": r.get("path"),  # WKT LineString
                    "timestamp": created_at_iso,
                }

                try:
                    # QoS 1 is a good default for results
                    client.publish(topic, json.dumps(payload), qos=1, retain=True)
                    logging.info(f"📤 Published → {topic}: {payload}")
                except Exception as e:
                    logging.error(f"❌ MQTT publish failed for {client_id}/{session_id}: {e}")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    logging.info("🚀 Starting Producer module…")
    client.loop_start()
    publish_results()
