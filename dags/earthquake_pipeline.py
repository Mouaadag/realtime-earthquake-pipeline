"""
=============================================================================
 DAG : earthquake_pipeline
 Description : Pipeline séismes temps réel
               USGS Earthquake API → Kafka (earthquake-raw)
               → Flink SQL → Kafka (earthquake-stats / earthquake-alerts)

 Séance 7 - TP Data Engineering Temps Réel
=============================================================================

 RAPPEL — Qu'est-ce qu'un fichier DAG ?
 ----------------------------------------
 Un DAG (Directed Acyclic Graph) Airflow est un fichier Python placé dans
 /opt/airflow/dags/. Le Scheduler le parse à intervalle régulier pour :
   1. Découvrir les tâches et leurs dépendances
   2. Créer des DagRuns selon le schedule défini
   3. Soumettre les TaskInstances à l'Executor

 Le code au niveau MODULE est exécuté à chaque parsing.
 Les fonctions (callables) sont exécutées uniquement quand la tâche tourne.

 SOURCE DE DONNÉES : USGS Earthquake API
 ----------------------------------------
 API publique, sans clé, maintenue par le U.S. Geological Survey.
 Documentation : https://earthquake.usgs.gov/fdsnws/event/1/
 Format de réponse : GeoJSON (RFC 7946)

 Exemple d'appel :
   GET /fdsnws/event/1/query?format=geojson
       &starttime=2026-01-01T00:00:00
       &endtime=2026-01-01T01:00:00
       &minmagnitude=2.5
=============================================================================
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ===========================================================================
#  CONFIGURATION
#
#  Évaluées au PARSING du DAG — garder ce bloc léger (pas de réseau, pas de DB).
# ===========================================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

KAFKA_TOPIC_RAW = "earthquake-raw"
KAFKA_TOPIC_ALERTS = "earthquake-alerts"

# ===========================================================================
#  DEFAULT ARGS
# ===========================================================================


# ===========================================================================
#  DAG DEFINITION
# ===========================================================================

# ===================================================================
#  TASK 1 : health_check
#  Vérifie que Kafka et Flink répondent avant de démarrer le pipeline.
# ===================================================================


# ===================================================================
#  TASK 2 : create_topics
#  Crée les 3 topics Kafka nécessaires au pipeline.
#
#  CONCEPT — Idempotence :
#  --if-not-exists garantit qu'on peut relancer cette tâche sans erreur.
#  C'est la règle d'or en orchestration : chaque tâche doit supporter
#  les retries et les re-runs sans créer de doublons ou d'erreurs.
#
#  Partitions :
#  - earthquake-raw      : 3 partitions (parallélisme ingestion)
#  - earthquake-enriched : 1 partition (ordre temporel requis)
#  - earthquake-alerts   : 1 partition (alertes, ordre temporel requis)
# ===================================================================


# ===================================================================
#  TASK 3 : run_flink_job
#  Soumet le fichier SQL au Flink SQL Client.
#
#  CONCEPT — Airflow orchestre, Flink traite :
#  Airflow ne traite pas la donnée lui-même — il délègue au cluster Flink
#  via "docker exec flink-sql-client bin/sql-client.sh -f <fichier>".
#  Le fichier SQL est monté dans le conteneur via le volume flink-jobs/.
# ===================================================================


# ===================================================================
#  TASK 4 : start_tableau_sink
#  Vérifie que le service Docker kafka-to-postgres est en vie.
#  Le service est déclaré dans docker-compose.yml avec
#  restart: unless-stopped — il démarre automatiquement avec la stack.
# ===================================================================


# ===================================================================
#  TASK 5 : verify_output
#  Vérifie que le pipeline Flink produit bien dans earthquake-enriched
#  et que PostgreSQL earthquake_events est peuplé par le producer.
# ===================================================================


# ===================================================================
#  DÉPENDANCES
#
#  Le producer (service Docker) alimente Kafka + PostgreSQL en continu.
#  Airflow orchestre uniquement la création des topics et Flink.
#
#  health_check → create_topics → run_flink_job → start_tableau_sink → verify_output
# ===================================================================
