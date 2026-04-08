"""
=============================================================================
 Producteur continu de séismes — temps réel

 Architecture :
   USGS API (poll toutes les 30s)
     → Kafka  (earthquake-raw)        ← consommé par Flink
     → PostgreSQL (earthquake_events) ← consommé par Tableau (map)

 Utilise le paramètre `updatedafter` de l'API USGS pour ne récupérer
 que les événements nouveaux ou mis à jour depuis le dernier poll.
 Le chevauchement de LOOKBACK_SEC évite de rater des événements en cas
 de latence côté USGS.
=============================================================================
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ===========================================================================
#  CONFIGURATION (via variables d'environnement injectées par Docker)
# ===========================================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_RAW = "earthquake-raw"

PG_HOST = os.environ.get("POSTGRES_HOST",     "airflow-postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB   = os.environ.get("POSTGRES_DB",       "airflow")
PG_USER = os.environ.get("POSTGRES_USER",     "airflow")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "airflow")

USGS_API_URL          = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MIN_MAGNITUDE         = 2.5
POLL_INTERVAL         = 30   # secondes entre chaque appel USGS
POLL_OVERLAP_SEC      = 90   # chevauchement entre polls (évite les trous)
INITIAL_LOOKBACK_DAYS = 7    # premier poll : remonte N jours pour amorcer la base


# ===========================================================================
#  HELPERS
# ===========================================================================

def _get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def _ensure_table(conn):
    """Crée earthquake_events si elle n'existe pas encore."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_events (
            event_id     TEXT PRIMARY KEY,
            magnitude    DOUBLE PRECISION,
            mag_type     TEXT,
            place        TEXT,
            event_time   TEXT,
            latitude     DOUBLE PRECISION,
            longitude    DOUBLE PRECISION,
            depth_km     DOUBLE PRECISION,
            significance INT,
            tsunami      INT,
            status       TEXT,
            type         TEXT,
            title        TEXT,
            ingested_at  TEXT
        )
    """)
    conn.commit()
    cur.close()
    logging.info("Table earthquake_events prête.")


def _parse_feature(feature: dict) -> dict:
    """Normalise un feature GeoJSON USGS en dict plat."""
    props  = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None, None])
    ts_ms  = props.get("time")
    ts_iso = (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        if ts_ms else None
    )
    return {
        "event_id":    props.get("code", feature.get("id", "unknown")),
        "magnitude":   props.get("mag"),
        "mag_type":    props.get("magType", "ml"),
        "place":       props.get("place", ""),
        "event_time":  ts_iso,
        "latitude":    coords[1],
        "longitude":   coords[0],
        "depth_km":    coords[2],
        "significance": props.get("sig", 0),
        "tsunami":     props.get("tsunami", 0),
        "status":      props.get("status", ""),
        "type":        props.get("type", "earthquake"),
        "title":       props.get("title", ""),
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def _wait_for_kafka(max_retries=20, delay=5):
    for attempt in range(max_retries):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            p.close()
            logging.info("Kafka disponible.")
            return
        except Exception:
            logging.warning("Kafka pas encore prêt (%d/%d)...", attempt + 1, max_retries)
            time.sleep(delay)
    raise RuntimeError("Kafka inaccessible après %d tentatives." % max_retries)


def _wait_for_postgres(max_retries=20, delay=5):
    for attempt in range(max_retries):
        try:
            conn = _get_pg_conn()
            conn.close()
            logging.info("PostgreSQL disponible.")
            return
        except Exception:
            logging.warning("PostgreSQL pas encore prêt (%d/%d)...", attempt + 1, max_retries)
            time.sleep(delay)
    raise RuntimeError("PostgreSQL inaccessible après %d tentatives." % max_retries)


# ===========================================================================
#  BOUCLE PRINCIPALE
# ===========================================================================

def run():
    logging.info(
        "Démarrage du producteur temps réel — poll USGS toutes les %ds.", POLL_INTERVAL
    )
    _wait_for_kafka()
    _wait_for_postgres()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )

    conn = _get_pg_conn()
    _ensure_table(conn)

    # Premier poll : seeding initial sur INITIAL_LOOKBACK_DAYS jours
    # (garantit d'avoir des données même si peu de séismes récents)
    # Les polls suivants utilisent POLL_OVERLAP_SEC pour ne pas rater d'événements.
    last_poll = datetime.now(tz=timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
    logging.info("Seeding initial : fenêtre de %d jours.", INITIAL_LOOKBACK_DAYS)

    while True:
        now = datetime.now(tz=timezone.utc)
        try:
            resp = requests.get(
                USGS_API_URL,
                params={
                    "format":       "geojson",
                    "starttime":    last_poll.strftime("%Y-%m-%dT%H:%M:%S"),
                    "endtime":      now.strftime("%Y-%m-%dT%H:%M:%S"),
                    "minmagnitude": MIN_MAGNITUDE,
                    "orderby":      "time",
                },
                timeout=15,
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])
            logging.info("%d séisme(s) récupérés (fenêtre %s → %s).",
                         len(features), last_poll.strftime("%H:%M:%S"), now.strftime("%H:%M:%S"))

            if features:
                cur = conn.cursor()
                for feature in features:
                    msg = _parse_feature(feature)

                    # → Kafka (consommé par Flink en temps réel)
                    producer.send(
                        KAFKA_TOPIC_RAW,
                        key=msg["event_id"].encode("utf-8"),
                        value=msg,
                    )

                    # → PostgreSQL (consommé par Tableau pour la map)
                    cur.execute(
                        """
                        INSERT INTO earthquake_events
                            (event_id, magnitude, mag_type, place, event_time,
                             latitude, longitude, depth_km, significance,
                             tsunami, status, type, title, ingested_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (
                            msg["event_id"],    msg["magnitude"],  msg["mag_type"],
                            msg["place"],       msg["event_time"],
                            msg["latitude"],    msg["longitude"],  msg["depth_km"],
                            msg["significance"], msg["tsunami"],
                            msg["status"],      msg["type"],
                            msg["title"],       msg["ingested_at"],
                        ),
                    )

                producer.flush()
                conn.commit()
                cur.close()
                logging.info("%d événement(s) publiés → Kafka + PostgreSQL.", len(features))

            # Polls suivants : overlap de POLL_OVERLAP_SEC pour éviter les trous
            last_poll = now - timedelta(seconds=POLL_OVERLAP_SEC)

        except requests.RequestException as exc:
            logging.error("Erreur API USGS : %s", exc)
        except psycopg2.Error as exc:
            logging.error("Erreur PostgreSQL : %s", exc)
            try:
                conn = _get_pg_conn()
            except Exception:
                pass

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
