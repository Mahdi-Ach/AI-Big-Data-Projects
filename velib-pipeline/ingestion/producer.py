"""
Producer Vélib' -> Kafka (semaine 1 : ingestion).

- station_status      : interrogé chaque minute, 1 message par station
                        (n'envoie rien si le flux n'a pas changé depuis le dernier appel)
- station_information : interrogé au démarrage puis toutes les heures

Chaque message Kafka est une enveloppe JSON :
    {"ingested_at": "...", "source_last_updated": 1234567890, "payload": {...station brute...}}
La donnée brute n'est pas modifiée : le nettoyage se fait plus tard (couche silver).

Lancer :  python producer.py
Variables d'environnement optionnelles :
    KAFKA_BOOTSTRAP (défaut localhost:29092), POLL_SECONDS (défaut 60)
"""
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
STATUS_URL = f"{BASE_URL}/station_status.json"
INFO_URL = f"{BASE_URL}/station_information.json"

TOPIC_STATUS = "velib.station_status"
TOPIC_INFO = "velib.station_information"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
INFO_REFRESH_SECONDS = 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("velib-producer")

running = True


def _stop(*_):
    global running
    running = False
    log.info("Arrêt demandé, fin du cycle en cours...")


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def on_delivery(err, msg):
    if err is not None:
        log.error("Échec d'envoi vers %s : %s", msg.topic(), err)


def fetch(url: str, retries: int = 3) -> dict | None:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("Appel %s échoué (tentative %d/%d) : %s", url, attempt, retries, exc)
            time.sleep(2 * attempt)
    return None


def publish(producer: Producer, topic: str, data: dict) -> int:
    """Envoie 1 message par station. Retourne le nombre de messages envoyés."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    source_ts = data.get("lastUpdatedOther")
    stations = data.get("data", {}).get("stations", [])
    for station in stations:
        envelope = {
            "ingested_at": ingested_at,
            "source_last_updated": source_ts,
            "payload": station,
        }
        producer.produce(
            topic,
            key=str(station.get("station_id", "")),
            value=json.dumps(envelope, ensure_ascii=False),
            on_delivery=on_delivery,
        )
        producer.poll(0)  # traite les callbacks sans bloquer
    producer.flush(10)
    return len(stations)


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    log.info("Producer démarré (Kafka : %s, intervalle : %ss)", BOOTSTRAP, POLL_SECONDS)

    last_status_ts = None
    last_info_refresh = 0.0

    while running:
        cycle_start = time.time()

        # Référentiel des stations (rarement modifié)
        if cycle_start - last_info_refresh >= INFO_REFRESH_SECONDS:
            info = fetch(INFO_URL)
            if info:
                n = publish(producer, TOPIC_INFO, info)
                log.info("station_information : %d stations envoyées", n)
                last_info_refresh = cycle_start

        # Disponibilités en temps réel
        status = fetch(STATUS_URL)
        if status:
            ts = status.get("lastUpdatedOther")
            if ts != last_status_ts:
                n = publish(producer, TOPIC_STATUS, status)
                log.info("station_status : %d stations envoyées (source ts=%s)", n, ts)
                last_status_ts = ts
            else:
                log.info("station_status : flux inchangé, rien à envoyer")

        # Attente jusqu'au prochain cycle (interruptible)
        while running and time.time() - cycle_start < POLL_SECONDS:
            time.sleep(1)

    producer.flush(10)
    log.info("Producer arrêté proprement.")


if __name__ == "__main__":
    main()