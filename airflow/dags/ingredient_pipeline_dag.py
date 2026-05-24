"""
ingredient_pipeline_dag.py

Linear DAG:
  ingest_pubchem → ingest_usda → dbt_seed → dbt_run → dbt_test

Runs daily. On failure the whole pipeline stops (no partial loads).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Paths (inside the Airflow container, match your volume mounts)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path("/opt/airflow/project")
INGESTION_DIR = PROJECT_ROOT / "ingestion"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
PYTHON_BIN = "/usr/local/bin/python3"
# dbt's CLI is installed as a pip console script in the airflow user's
# local bin. `python -m dbt` doesn't work (the package has no __main__),
# and `python -m dbt.cli.main` emits a RuntimeWarning. Just use the
# script directly.
DBT_BIN = "/home/airflow/.local/bin/dbt"

# Diagnostic preamble — prepend to a bash_command to capture the exact
# environment the BashOperator runs in. We've seen ingestion succeed
# under `docker compose exec` but fail under the DAG; comparing this
# output against a known-good manual run is the fastest way to find why.
DIAGNOSTICS = r"""
echo "=== DAG diagnostics ==="
echo "host:            $(hostname)"
echo "user:            $(id)"
echo "cwd:             $(pwd)"
echo "python:          $(command -v python3) ($(python3 -V 2>&1))"
echo "PATH:            $PATH"
echo "proxy vars:"
env | grep -iE '(_proxy|proxy_)' | sed 's/^/  /' || echo "  (none)"
echo "DUCKDB_PATH:     ${DUCKDB_PATH:-<unset>}"
echo "ingestion dir:   $(ls -la /opt/airflow/project/ingestion 2>&1 | head -5)"
echo "DNS check:       $(getent hosts pubchem.ncbi.nlm.nih.gov || echo FAILED)"
echo "TCP reachability:"
curl -sS --max-time 10 -o /dev/null \
  -w "  pubchem: HTTP %{http_code} in %{time_total}s\n" \
  https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/2519/property/MolecularFormula/JSON \
  || echo "  pubchem unreachable"
echo "httpx version:   $(python3 -c 'import httpx; print(httpx.__version__)' 2>&1)"
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
    description="Ingest PubChem + USDA → dbt transform → dbt test",
    schedule="@daily",
    start_date=datetime(2026, 5, 24),
    catchup=False,
    # DuckDB takes an exclusive write lock on the .duckdb file. Two
    # overlapping runs (e.g. a retry plus a scheduled run) would deadlock
    # on the lock; serialize runs at the DAG level instead.
    max_active_runs=1,
    default_args=default_args,
    tags=["ingredient", "dbt", "research"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: ingest from PubChem
    # ------------------------------------------------------------------
    ingest_pubchem = BashOperator(
        task_id="ingest_pubchem",
        bash_command=(
            DIAGNOSTICS
            + f"\nset -x\n{PYTHON_BIN} -u {INGESTION_DIR}/pubchem_fetch.py"
        ),
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 2: ingest from USDA
    # ------------------------------------------------------------------
    ingest_usda = BashOperator(
        task_id="ingest_usda",
        bash_command=(
            DIAGNOSTICS
            + f"\nset -x\n{PYTHON_BIN} -u {INGESTION_DIR}/usda_fetch.py"
        ),
        # USDA_API_KEY is injected into the worker container by
        # docker-compose from .env, so append_env=True makes it visible
        # to the script via os.getenv. No Airflow Variable needed.
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 3: load dbt seed data
    # ------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} seed --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 4: run dbt models (staging → intermediate → marts)
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 5: run dbt tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test --profiles-dir .",
        env={"DUCKDB_PATH": str(PROJECT_ROOT / "data/ingredient.duckdb")},
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Linear dependency chain
    # ------------------------------------------------------------------
    ingest_pubchem >> ingest_usda >> dbt_seed >> dbt_run >> dbt_test
