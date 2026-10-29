# producer/velib_producer.py
import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

GBFS_DISCOVERY_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json"
KAFKA_BROKER = "localhost:29092"
INTERVAL_SECONDS = 60

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
)

def get_feed_urls():
    resp = requests.get(GBFS_DISCOVERY_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    feeds = {}
    for lang, lang_data in data.get("data", {}).items():
        for feed in lang_data.get("feeds", []):
            feeds[feed["name"]] = feed["url"]
        break
    return feeds

def fetch_and_send(url, topic):
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        envelope = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        producer.send(topic, value=envelope)
        producer.flush()
        print(f"[{datetime.now(timezone.utc).isoformat()}] Sent to {topic}")
    except Exception as e:
        print(f"Error fetching/sending {topic}: {e}")

def main():
    print("Démarrage du producer Vélib...")
    while True:
        try:
            feeds = get_feed_urls()
            info_url = feeds.get("station_information")
            status_url = feeds.get("station_status")
            if info_url:
                fetch_and_send(info_url, "velib.station_information")
            if status_url:
                fetch_and_send(status_url, "velib.station_status")
        except Exception as e:
            print(f"Erreur dans la boucle principale: {e}")
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
