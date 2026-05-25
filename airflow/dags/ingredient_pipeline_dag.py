"""
ingredient_pipeline_dag.py

Linear DAG:
  ingest_pubchem → ingest_usda → dbt_run → dbt_test

Runs daily. On failure the whole pipeline stops (no partial loads).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Paths (inside the Airflow container, match your volume mounts)
# ---------------------------------------------------------------------------
PROJECT_ROOT    = Path("/opt/airflow/project")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
PYTHON_BIN      = "/usr/local/bin/python3"
DBT_BIN         = "/home/airflow/.local/bin/dbt"

# Diagnostic preamble
DIAGNOSTICS = r"""
echo "=== DAG diagnostics ==="
echo "host:        $(hostname)"
echo "user:        $(id)"
echo "python:      $(command -v python3) ($(python3 -V 2>&1))"
echo "GCP_PROJECT: ${GCP_PROJECT:-<unset>}"
echo "GCP_DATASET: ${GCP_DATASET:-<unset>}"
echo "credentials: ${GOOGLE_APPLICATION_CREDENTIALS:-<unset>}"
echo "keyfile exists: $(test -f ${GOOGLE_APPLICATION_CREDENTIALS} && echo YES || echo NO)"
echo "=== end diagnostics ==="
"""

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
    description="Ingest PubChem + USDA → BigQuery → dbt transform → dbt test",
    schedule="@daily",
    start_date=datetime(2026, 5, 24),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingredient", "dbt", "bigquery", "research"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: ingest from PubChem → BigQuery raw_compounds
    # ------------------------------------------------------------------
    ingest_pubchem = BashOperator(
        task_id="ingest_pubchem",
        bash_command=(
            DIAGNOSTICS
            + f"\nset -x\ncd {PROJECT_ROOT} && {PYTHON_BIN} -u run.py --source pubchem"
        ),
    )

    # ------------------------------------------------------------------
    # Task 2: ingest from USDA → BigQuery raw_nutrition
    # ------------------------------------------------------------------
    ingest_usda = BashOperator(
        task_id="ingest_usda",
        bash_command=(
            DIAGNOSTICS
            + f"\nset -x\ncd {PROJECT_ROOT} && {PYTHON_BIN} -u run.py --source usda"
        ),
    )

    # ------------------------------------------------------------------
    # Task 3: run dbt models
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --profiles-dir .",
    )

    # ------------------------------------------------------------------
    # Task 4: run dbt tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test --profiles-dir .",
    )

    # ------------------------------------------------------------------
    # Linear dependency chain
    # ------------------------------------------------------------------
    ingest_pubchem >> ingest_usda >> dbt_run >> dbt_test