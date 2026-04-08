"""
Tests de structure et de configuration du DAG earthquake_pipeline.

Ces tests vérifient que le DAG est valide sans exécuter aucune tâche
ni se connecter à Kafka, Flink ou PostgreSQL.
"""

import pytest
from airflow.models import DagBag


DAG_ID = "earthquake_pipeline"
DAG_FILE = "dags/earthquake_pipeline.py"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag(DAG_ID)


# ---------------------------------------------------------------------------
#  Chargement
# ---------------------------------------------------------------------------

def test_dag_loads_without_errors(dagbag):
    """Le fichier DAG ne doit contenir aucune erreur de parsing."""
    assert DAG_FILE not in dagbag.import_errors, (
        f"Erreur d'import dans {DAG_FILE} : {dagbag.import_errors.get(DAG_FILE)}"
    )


def test_dag_exists(dag):
    """Le DAG earthquake_pipeline doit être présent dans le DagBag."""
    assert dag is not None, f"DAG '{DAG_ID}' introuvable."


# ---------------------------------------------------------------------------
#  Configuration du DAG
# ---------------------------------------------------------------------------

def test_dag_schedule(dag):
    """Le DAG doit s'exécuter toutes les 30 minutes."""
    assert dag.schedule_interval == "*/30 * * * *"


def test_dag_no_catchup(dag):
    """catchup doit être désactivé pour éviter les backfills non voulus."""
    assert dag.catchup is False


def test_dag_max_active_runs(dag):
    """max_active_runs = 1 pour éviter les exécutions parallèles."""
    assert dag.max_active_runs == 1


def test_dag_tags(dag):
    """Les tags attendus doivent être présents."""
    assert {"earthquake", "kafka", "flink"}.issubset(set(dag.tags))


# ---------------------------------------------------------------------------
#  Tâches
# ---------------------------------------------------------------------------

EXPECTED_TASKS = {"health_check", "create_topics", "run_flink_job", "start_tableau_sink", "verify_output"}


def test_dag_task_count(dag):
    """Le DAG doit contenir exactement 5 tâches."""
    assert len(dag.tasks) == len(EXPECTED_TASKS)


def test_dag_task_ids(dag):
    """Les 4 tâches attendues doivent toutes être présentes."""
    assert set(dag.task_ids) == EXPECTED_TASKS


# ---------------------------------------------------------------------------
#  Dépendances (ordre d'exécution)
# ---------------------------------------------------------------------------

def test_linear_chain(dag):
    """Le DAG doit former une chaîne linéaire sans branche parallèle."""
    chain = ["health_check", "create_topics", "run_flink_job", "verify_output"]
    for i, task_id in enumerate(chain[:-1]):
        task = dag.get_task(task_id)
        downstream_ids = {t.task_id for t in task.downstream_list}
        assert chain[i + 1] in downstream_ids, (
            f"'{task_id}' devrait pointer vers '{chain[i + 1]}'"
        )


# ---------------------------------------------------------------------------
#  Default args
# ---------------------------------------------------------------------------

def test_default_args_retries(dag):
    """Chaque tâche doit avoir 2 retries configurés."""
    for task in dag.tasks:
        assert task.retries == 2, f"Tâche '{task.task_id}' : retries attendu = 2"


def test_run_flink_job_sql_param(dag):
    """run_flink_job doit pointer vers le fichier SQL attendu."""
    task = dag.get_task("run_flink_job")
    assert task.params["sql_file"] == "/opt/flink/jobs/earthquake_processing.sql"
