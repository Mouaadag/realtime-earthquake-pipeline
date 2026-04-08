# Real-Time Earthquake Pipeline

Pipeline de données temps réel complet : collecte de séismes via l'API USGS, transport via Kafka, traitement stream avec Apache Flink SQL, orchestration Airflow et visualisation Tableau Desktop.

---

## Architecture

```
USGS Earthquake API (GeoJSON, sans clé)
  └─▶ earthquake-producer (Docker, poll 30s)
        ├─▶ Kafka ──── earthquake-raw (3 partitions)
        │                   └─▶ Apache Flink SQL
        │                         ├─▶ earthquake-enriched  ──┐
        │                         └─▶ earthquake-alerts    ──┤
        │                                                     │
        │                                         kafka_to_postgres.py
        │                                                     │
        └─▶ PostgreSQL ── earthquake_events                   └─▶ PostgreSQL
                              └─▶ Tableau (carte géo)               ├─▶ earthquake_stats
                                                                     ├─▶ earthquake_by_depth
                                                                     └─▶ earthquake_alerts
                                                                           └─▶ Tableau Desktop
```

**Orchestration :** Airflow DAG `earthquake_pipeline` — toutes les 30 minutes

---

## Stack technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Message broker | Apache Kafka (KRaft) | 7.6.0 |
| Stream processing | Apache Flink SQL | 1.18 |
| Orchestration | Apache Airflow | 2.10.4 |
| Base de données | PostgreSQL | 15 |
| Schéma | Confluent Schema Registry | 7.6.0 |
| UI Kafka | Kafdrop | latest |
| Visualisation | Tableau Desktop | — |
| Runtime | Docker / Docker Compose | — |

---

## Prérequis

- Docker Desktop (≥ 4.x) avec au moins **6 Go de RAM** alloués
- Docker Compose v2
- Python 3.11+ (pour `kafka_to_postgres.py` en local)
- Tableau Desktop (optionnel, pour la visualisation)

---

## Démarrage rapide

### 1. Cloner le dépôt

```bash
git clone <repo-url>
cd real-time-pipeline
```

### 2. Lancer l'infrastructure

```bash
cd infra
docker compose up -d
```

Le premier démarrage est long (~5-10 min) : les images Flink téléchargent une quinzaine de JARs.

### 3. Vérifier que tout est prêt

```bash
# Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Airflow
open http://localhost:8080          # admin / admin

# Flink
open http://localhost:8082          # UI Flink

# Kafdrop (UI Kafka)
open http://localhost:9090
```

### 4. Déclencher le DAG manuellement

Dans l'UI Airflow → DAG `earthquake_pipeline` → bouton **Trigger DAG**.

Ou via CLI :

```bash
docker exec airflow-webserver airflow dags trigger earthquake_pipeline
```

### 5. (Optionnel) Alimenter Tableau Desktop

Lancer le script de sink local :

```bash
pip install kafka-python psycopg2-binary
python scripts/kafka_to_postgres.py
```

Connexion Tableau : `localhost:5433` · base `earthquake` · login `airflow / airflow`

---

## Structure du projet

```
real-time-pipeline/
├── dags/
│   └── earthquake_pipeline.py      # DAG Airflow (orchestration)
├── producer/
│   └── earthquake_producer.py      # Producteur temps réel USGS → Kafka + PostgreSQL
├── flink-jobs/
│   └── earthquake_processing.sql   # Jobs Flink SQL (enrichissement + alertes)
├── scripts/
│   └── kafka_to_postgres.py        # Sink Kafka → PostgreSQL pour Tableau
├── infra/
│   └── docker-compose.yml          # Infrastructure complète
└── docs/
    └── tp_earthquake.pdf           # Énoncé du TP
```

---

## Composants détaillés

### Producer (`producer/earthquake_producer.py`)

Service Docker qui tourne en **boucle infinie** :

- Poll l'API USGS toutes les **30 secondes** (séismes ≥ magnitude 2.5)
- Overlap de **90 secondes** entre chaque fenêtre pour ne rater aucun événement
- Au premier démarrage : remonte **7 jours** en arrière pour amorcer la base
- Publie chaque séisme dans **Kafka** (`earthquake-raw`) et **PostgreSQL** (`earthquake_events`)

Champs produits par événement : `event_id`, `magnitude`, `mag_type`, `place`, `event_time`, `latitude`, `longitude`, `depth_km`, `significance`, `tsunami`, `status`, `type`, `title`, `ingested_at`

---

### Traitement Stream (`flink-jobs/earthquake_processing.sql`)

Deux jobs Flink SQL sans fenêtre temporelle — chaque événement entrant produit immédiatement une sortie.

> Les séismes sont rares (~1 toutes les 15-30 min). Une fenêtre TUMBLE de 60s produirait des sorties vides.

**JOB 1 — Enrichissement** (`earthquake-raw` → `earthquake-enriched`)

| Champ ajouté | Logique |
|---|---|
| `depth_category` | `< 70 km` → Superficiel · `70-300 km` → Intermédiaire · `> 300 km` → Profond |
| `severity` | `tsunami=1` → TSUNAMI · `≥ 8.0` → Dévastateur · `≥ 7.0` → Majeur · `≥ 6.0` → Fort · `≥ 5.0` → Modéré · sinon → Mineur |
| `is_significant` | `1` si `significance > 500` (seuil officiel USGS) |
| `processed_at` | Timestamp de traitement Flink |

**JOB 2 — Alertes** (`earthquake-raw` → `earthquake-alerts`)

Filtre `magnitude ≥ 5.0 OR tsunami = 1` et émet immédiatement une alerte avec niveau de sévérité.

---

### DAG Airflow (`dags/earthquake_pipeline.py`)

Planifié toutes les **30 minutes** (`*/30 * * * *`), 4 tâches séquentielles :

```
health_check → create_topics → run_flink_job → verify_output
```

| Tâche | Description |
|---|---|
| `health_check` | Vérifie que Kafka et Flink répondent |
| `create_topics` | Crée les 3 topics Kafka (`--if-not-exists` → idempotent) |
| `run_flink_job` | Soumet `earthquake_processing.sql` au Flink SQL Client |
| `verify_output` | Vérifie la présence de messages dans `earthquake-enriched` et de lignes dans PostgreSQL |

---

### Sink Tableau (`scripts/kafka_to_postgres.py`)

Script local qui consomme les topics de résultats Flink et les matérialise en PostgreSQL (Tableau ne peut pas se connecter directement à Kafka).

| Topic Kafka | Table PostgreSQL |
|---|---|
| `earthquake-stats` | `earthquake_stats` |
| `earthquake-by-depth` | `earthquake_by_depth` |
| `earthquake-alerts` | `earthquake_alerts` |

---

## Topics Kafka

| Topic | Partitions | Producteur | Consommateur |
|---|---|---|---|
| `earthquake-raw` | 3 | `earthquake-producer` | Flink SQL |
| `earthquake-enriched` | 1 | Flink JOB 1 | `kafka_to_postgres.py` |
| `earthquake-alerts` | 1 | Flink JOB 2 | `kafka_to_postgres.py` |

---

## Tables PostgreSQL

| Table | Alimentée par | Usage |
|---|---|---|
| `earthquake_events` | `earthquake-producer` | Carte géographique Tableau |
| `earthquake_stats` | `kafka_to_postgres.py` | Statistiques par fenêtre |
| `earthquake_by_depth` | `kafka_to_postgres.py` | Répartition par profondeur |
| `earthquake_alerts` | `kafka_to_postgres.py` | Alertes temps réel |

---

## Ports exposés

| Service | URL / Port |
|---|---|
| Airflow UI | http://localhost:8080 |
| Flink UI | http://localhost:8082 |
| Kafdrop (UI Kafka) | http://localhost:9090 |
| Schema Registry | http://localhost:8081 |
| ksqlDB | http://localhost:8088 |
| PostgreSQL | localhost:5433 |
| Kafka (externe) | localhost:9092 |

---

## Commandes utiles

```bash
# Voir les logs du producer
docker logs earthquake-producer --follow

# Consommer earthquake-raw en temps réel
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic earthquake-raw \
  --from-beginning

# Compter les séismes en base
docker exec airflow-postgres psql -U airflow -d airflow \
  -c "SELECT COUNT(*) FROM earthquake_events;"

# Forcer la migration de la base Airflow
docker compose -f infra/docker-compose.yml up --force-recreate airflow-init

# Arrêter proprement
docker compose -f infra/docker-compose.yml down
```

---

## Dépannage

**`airflow-webserver` en boucle de redémarrage avec `db upgrade` error**

```bash
docker compose -f infra/docker-compose.yml up --force-recreate airflow-init
docker compose -f infra/docker-compose.yml up -d airflow-webserver
```

**`Can't locate revision` dans les logs Airflow**

La base a été initialisée avec une version d'Airflow plus récente. Mettre à jour l'image dans `docker-compose.yml` (`apache/airflow:2.10.4-python3.11`) et rebuild :

```bash
docker compose -f infra/docker-compose.yml build airflow-init airflow-webserver airflow-scheduler
docker compose -f infra/docker-compose.yml up --force-recreate airflow-init
docker compose -f infra/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

**Le producer ne publie rien**

```bash
docker logs earthquake-producer --follow
# Vérifier que Kafka est healthy
docker inspect kafka --format='{{.State.Health.Status}}'
```
