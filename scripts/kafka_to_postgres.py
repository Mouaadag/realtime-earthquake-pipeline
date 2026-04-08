"""
=============================================================================
 Script : kafka_to_postgres.py
 Description : Consomme les topics Kafka earthquake-* et insère dans PostgreSQL
               pour permettre la connexion Tableau Desktop.

 Usage :
   pip install kafka-python psycopg2-binary
   python scripts/kafka_to_postgres.py

 Tableau se connecte ensuite via :
   Serveur : localhost
   Port    : 5433
   Base    : earthquake
   Login   : airflow / airflow
=============================================================================
"""

import json
import logging
import os
import signal
import threading

import psycopg2
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
#  Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

PG_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5433")),
    "dbname": os.environ.get("POSTGRES_DB", "earthquake"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------------
#  Initialisation du schéma PostgreSQL
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS earthquake_enriched (
    id              SERIAL PRIMARY KEY,
    event_id        TEXT UNIQUE,
    magnitude       DOUBLE PRECISION,
    mag_type        TEXT,
    place           TEXT,
    event_time      TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    depth_km        DOUBLE PRECISION,
    significance    INT,
    tsunami         INT,
    status          TEXT,
    title           TEXT,
    depth_category  TEXT,
    severity        TEXT,
    is_significant  INT,
    processed_at    TEXT,
    inserted_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS earthquake_stats (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    nb_earthquakes  BIGINT,
    mag_avg         DOUBLE PRECISION,
    mag_max         DOUBLE PRECISION,
    mag_min         DOUBLE PRECISION,
    depth_avg_km    DOUBLE PRECISION,
    nb_tsunami      BIGINT,
    nb_significant  BIGINT,
    inserted_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS earthquake_by_depth (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    depth_category  TEXT,
    nb_earthquakes  BIGINT,
    mag_avg         DOUBLE PRECISION,
    mag_max         DOUBLE PRECISION,
    inserted_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS earthquake_alerts (
    id          SERIAL PRIMARY KEY,
    event_id    TEXT UNIQUE,
    event_time  TIMESTAMPTZ,
    magnitude   DOUBLE PRECISION,
    place       TEXT,
    depth_km    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    latitude    DOUBLE PRECISION,
    tsunami     INT,
    severity    TEXT,
    alert_time  TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    logging.info("Schéma PostgreSQL initialisé.")


# ---------------------------------------------------------------------------
#  Fonctions d'insertion par topic
# ---------------------------------------------------------------------------


def insert_enriched(cur, msg: dict):
    cur.execute(
        """
        INSERT INTO earthquake_enriched
            (event_id, magnitude, mag_type, place, event_time, latitude, longitude,
             depth_km, significance, tsunami, status, title,
             depth_category, severity, is_significant, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            msg.get("event_id"),
            msg.get("magnitude"),
            msg.get("mag_type"),
            msg.get("place"),
            msg.get("event_time"),
            msg.get("latitude"),
            msg.get("longitude"),
            msg.get("depth_km"),
            msg.get("significance"),
            msg.get("tsunami"),
            msg.get("status"),
            msg.get("title"),
            msg.get("depth_category"),
            msg.get("severity"),
            msg.get("is_significant"),
            msg.get("processed_at"),
        ),
    )


def insert_stats(cur, msg: dict):
    cur.execute(
        """
        INSERT INTO earthquake_stats
            (window_start, window_end, nb_earthquakes, mag_avg, mag_max,
             mag_min, depth_avg_km, nb_tsunami, nb_significant)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            msg.get("window_start"),
            msg.get("window_end"),
            msg.get("nb_earthquakes"),
            msg.get("mag_avg"),
            msg.get("mag_max"),
            msg.get("mag_min"),
            msg.get("depth_avg_km"),
            msg.get("nb_tsunami"),
            msg.get("nb_significant"),
        ),
    )


def insert_by_depth(cur, msg: dict):
    cur.execute(
        """
        INSERT INTO earthquake_by_depth
            (window_start, window_end, depth_category, nb_earthquakes, mag_avg, mag_max)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            msg.get("window_start"),
            msg.get("window_end"),
            msg.get("depth_category"),
            msg.get("nb_earthquakes"),
            msg.get("mag_avg"),
            msg.get("mag_max"),
        ),
    )


def insert_alert(cur, msg: dict):
    cur.execute(
        """
        INSERT INTO earthquake_alerts
            (event_id, event_time, magnitude, place, depth_km, longitude, latitude,
             tsunami, severity, alert_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            msg.get("event_id"),
            msg.get("event_time"),
            msg.get("magnitude"),
            msg.get("place"),
            msg.get("depth_km"),
            msg.get("longitude"),
            msg.get("latitude"),
            msg.get("tsunami"),
            msg.get("severity"),
            msg.get("alert_time"),
        ),
    )


TOPIC_HANDLERS = {
    "earthquake-enriched": insert_enriched,
    "earthquake-stats":    insert_stats,
    "earthquake-by-depth": insert_by_depth,
    "earthquake-alerts":   insert_alert,
}


# ---------------------------------------------------------------------------
#  Boucle principale de consommation
# ---------------------------------------------------------------------------


def consume(stop_event: threading.Event):
    # Connexion PostgreSQL
    conn = psycopg2.connect(**PG_CONFIG)
    init_schema(conn)

    # Consumer Kafka — lit depuis le début, groupe dédié
    consumer = KafkaConsumer(
        *TOPIC_HANDLERS.keys(),
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="tableau-sink",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=2000,  # itère même si pas de nouveau message
    )

    logging.info("Consommation démarrée sur : %s", list(TOPIC_HANDLERS.keys()))
    inserted = 0

    while not stop_event.is_set():
        try:
            for record in consumer:
                if stop_event.is_set():
                    break
                handler = TOPIC_HANDLERS.get(record.topic)
                if handler is None:
                    continue
                try:
                    with conn.cursor() as cur:
                        handler(cur, record.value)
                    conn.commit()
                    inserted += 1
                    if inserted % 10 == 0:
                        logging.info("%d messages insérés dans PostgreSQL.", inserted)
                except Exception as e:
                    conn.rollback()
                    logging.error("Erreur insertion topic=%s : %s", record.topic, e)
        except Exception:
            # consumer_timeout_ms atteint — boucle normale
            pass

    consumer.close()
    conn.close()
    logging.info("Consommation arrêtée. Total inséré : %d.", inserted)


# ---------------------------------------------------------------------------
#  Point d'entrée
# ---------------------------------------------------------------------------


def main():
    # Création de la base 'earthquake' si elle n'existe pas
    conn0 = psycopg2.connect(**{**PG_CONFIG, "dbname": "airflow"})
    conn0.autocommit = True
    with conn0.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'earthquake'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE earthquake")
            logging.info("Base 'earthquake' créée.")
    conn0.close()

    stop_event = threading.Event()

    def handle_sigint(sig, frame):
        logging.info("Arrêt demandé (CTRL+C)...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    consume(stop_event)


if __name__ == "__main__":
    main()
