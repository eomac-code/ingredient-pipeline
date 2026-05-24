"""
ingredient_pipeline_dag.py

Linear DAG:
  ingest_pubchem → ingest_usda → dbt_seed → dbt_run → dbt_test

Runs daily. On failure the whole pipeline stops (no partial loads).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Paths (inside the Airflow container, match your volume mounts)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path("/opt/airflow/project")
INGESTION_DIR = PROJECT_ROOT / "ingestion"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
PYTHON_BIN = "/opt/airflow/project/.venv/bin/python"
DBT_BIN = "/opt/airflow/project/.venv/bin/dbt"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ingredient_pipeline",
    description="Ingest PubChem + USDA → dbt transform → dbt test",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingredient", "dbt", "research"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: ingest from PubChem
    # ------------------------------------------------------------------
    ingest_pubchem = BashOperator(
        task_id="ingest_pubchem",
        bash_command=f"{PYTHON_BIN} {INGESTION_DIR}/pubchem_fetch.py",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
    )

    # ------------------------------------------------------------------
    # Task 2: ingest from USDA
    # ------------------------------------------------------------------
    ingest_usda = BashOperator(
        task_id="ingest_usda",
        bash_command=f"{PYTHON_BIN} {INGESTION_DIR}/usda_fetch.py",
        env={
            "DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb"),
            "USDA_API_KEY": "{{ var.value.usda_api_key }}",
        },
    )

    # ------------------------------------------------------------------
    # Task 3: load dbt seed data
    # ------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} seed --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
    )

    # ------------------------------------------------------------------
    # Task 4: run dbt models (staging → intermediate → marts)
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
    )

    # ------------------------------------------------------------------
    # Task 5: run dbt tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
    )

    # ------------------------------------------------------------------
    # Linear dependency chain
    # ------------------------------------------------------------------
    ingest_pubchem >> ingest_usda >> dbt_seed >> dbt_run >> dbt_test
