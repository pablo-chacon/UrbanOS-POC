import os
import json
import time
import logging
from typing import Set, Tuple

import paho.mqtt.client as mqtt
from db.db_connection import fetch_optimized_route

logging.basicConfig(level=logging.INFO)

BROKER = os.getenv("MQTT_BROKER", "mqtt-broker")
PORT = int(os.getenv("MQTT_PORT", 1883))
# Accept both styles; normalize to no trailing slash when formatting
RESULTS_TOPIC_TEMPLATE = (os.getenv("MQTT_RESULTS_TOPIC", "results/client/{client_id}")).rstrip("/")

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

# simple in-process dedupe: (client_id, created_at_iso)
_seen: Set[Tuple[str, str]] = set()


def publish_results(poll_seconds: int = 5):
    """Poll DB for fresh unified routes and publish to per-client topic."""
    while True:
        rows = fetch_optimized_route()
        if not rows:
            logging.info("⏳ No fresh routes to publish.")
        else:
            for r in rows:
                client_id = str(r["client_id"])
                created_at_iso = r["created_at"].isoformat() if r.get("created_at") else ""
                key = (client_id, created_at_iso)

                # skip if we already sent this one during this process lifetime
                if key in _seen:
                    continue
                _seen.add(key)

                topic = RESULTS_TOPIC_TEMPLATE.format(client_id=client_id)
                payload = {
                    "client_id": client_id,
                    "stop_id": r.get("stop_id"),
                    "destination": {
                        "lat": r.get("destination_lat"),
                        "lon": r.get("destination_lon"),
                    },
                    # WKT LineString
                    "route_path": r.get("path"),
                    "timestamp": created_at_iso,
                }

                try:
                    client.publish(topic, json.dumps(payload))
                    logging.info(f"📤 Published → {topic}: {payload}")
                except Exception as e:
                    logging.error(f"❌ MQTT publish failed for {client_id}: {e}")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    logging.info("🚀 Starting Producer module…")
    client.loop_start()
    publish_results()
