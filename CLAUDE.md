# ingredient-pipeline

A data pipeline that ingests food ingredient data from PubChem and USDA FoodData Central, transforms it with dbt, and serves a final ingredient catalog for flavor/nutrition R&D.

## Architecture

```
PubChem API          USDA FoodData Central API
     |                          |
pubchem_fetch.py         usda_fetch.py
     |                          |
raw_compounds (DuckDB)   raw_nutrition (DuckDB)         schema: main
     |                          |
  stg_compounds           stg_nutrition                 schema: main_staging  (views)
          \                   /
        int_ingredient_enriched                         (ephemeral, fuzzy join — not materialised)
                   |
          mart_ingredient_catalog                       schema: main_marts    (table)
```

**Orchestration:** Airflow 3.2.1 DAG (`ingredient_pipeline_dag.py`) runs daily, CeleryExecutor:
`ingest_pubchem → ingest_usda → dbt_seed → dbt_run → dbt_test`

`max_active_runs=1` to prevent overlapping runs from contending on DuckDB's exclusive write lock.

## Stack

- **Python 3.12**, httpx 0.27, tenacity, duckdb 1.0, dbt-core 1.11, dbt-duckdb 1.10
- **Airflow 3.2.1** (CeleryExecutor via Docker: webserver/api-server, scheduler, worker, triggerer, postgres, redis)
- SQLFluff for SQL linting, pytest, great-expectations

## Container paths (apache/airflow:3.2.1-python3.12)

| Thing | Path |
|---|---|
| Project root (host bind-mounted) | `/opt/airflow/project` |
| Python interpreter | `/usr/local/bin/python3` |
| dbt CLI | `/home/airflow/.local/bin/dbt` (installed as user-local pip console script) |
| DuckDB file | `/opt/airflow/project/data/ingredient.duckdb` |
| Worker user site-packages | `/home/airflow/.local/lib/python3.12/site-packages/` |

## Environment variables

| Variable | Default | Where set | Description |
|---|---|---|---|
| `DUCKDB_PATH` | **none — required** | DAG BashOperator env (absolute path) | Path to DuckDB file. `profiles.yml` has no fallback — relative defaults cause stray `.duckdb` files. |
| `USDA_API_KEY` | `DEMO_KEY` | `.env` → docker-compose → container | USDA FDC API key |
| `AIRFLOW_JWT_SECRET` | **none — required** | `.env` → docker-compose | Shared JWT signing secret for Airflow 3.x Task SDK auth |
| `AIRFLOW_ADMIN_PASSWORD` | **none — required** | `.env` → docker-compose | Simple-auth-manager admin password |
| `AIRFLOW_UID` | `50000` | `.env` | UID the airflow containers run as |

`.env` → container flow: docker-compose substitutes `${VAR}` from `.env`, but a variable is only visible *inside* a container if it's also declared under that service's `environment:` block.

## dbt model layers + schemas

- `staging/` — **views** in `main_staging`; cast + rename raw sources, filter nulls. 1:1 with raw.
- `intermediate/` — **ephemeral**; fuzzy join on IUPAC name ↔ USDA search term. No materialised object — inlined into mart at compile time.
- `marts/` — **tables** in `main_marts`; adds Lipinski rule-of-five, polarity classification, FAIR PubChem URLs, data-quality flags, `dbt_updated_at`.

To query the warehouse, **schema-qualify** everything (`main.raw_compounds`, `main_staging.stg_compounds`, `main_marts.mart_ingredient_catalog`); unqualified names only resolve in `main`.

## Local setup

```bash
bash setup.sh
# Required in .env: USDA_API_KEY, AIRFLOW_JWT_SECRET, AIRFLOW_ADMIN_PASSWORD, AIRFLOW_UID
```

## Inspecting the DuckDB

Always open **read-only** so you don't fight the DAG for the exclusive lock:

```bash
# from inside the worker (no host install needed)
docker compose exec airflow-worker python3 -c "
import duckdb
con = duckdb.connect('/opt/airflow/project/data/ingredient.duckdb', read_only=True)
con.sql('SELECT * FROM main_marts.mart_ingredient_catalog').show()
"

# or with duckdb CLI on the host (brew install duckdb)
duckdb -readonly data/ingredient.duckdb
```

---

## Gotchas + lessons learned

These are non-obvious things this codebase has been bitten by. Read before debugging.

### Airflow 3.x runs tasks via an HTTP Task SDK, not direct DB access

Airflow 3.x workers no longer talk to Postgres directly. They make HTTPS calls (via httpx) to the **api-server's `/execution/` endpoint** to register task starts/stops. Two compose-level configs are required for this to work:

1. `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-webserver:8080/execution/` — without this the worker tries `localhost:8080` inside its own container, gets ECONNREFUSED, and **the task log stays 0 bytes** (the task body never starts).
2. `AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}` — a *shared* JWT signing secret across all containers. If unset, each container generates its own at startup and the worker's token fails verification with `Invalid auth token: Signature verification failed`.

If you see `httpx.ConnectError` or `ServerResponseError` from inside `airflow/sdk/api/client.py`, the problem is Airflow's own internals, **not** your DAG's HTTP calls.

### DuckDB takes an exclusive write lock

Only one writer at a time. Consequences:

- The DAG sets `max_active_runs=1` so two runs can't deadlock on the lock.
- Never open the file in read-write mode while the DAG is running (`duckdb` CLI defaults to RW). Always pass `read_only=True` / `-readonly`.
- A stale Python process from a crashed task can hold the lock. `docker compose up -d --force-recreate airflow-worker` kills it.

### `DUCKDB_PATH` must be absolute and set explicitly

`profiles.yml` deliberately has no default. The previous default `'../data/ingredient.duckdb'` produced stray `dbt_project/ingredient.duckdb` files whenever dbt was invoked from a different CWD. The DAG BashOperators all set an absolute `DUCKDB_PATH`; manual dbt runs must too:

```bash
DUCKDB_PATH=$(pwd)/data/ingredient.duckdb \
  dbt run --project-dir dbt_project --profiles-dir dbt_project
```

### dbt invocation

`python -m dbt` does **not** work (package has no `__main__`). `python -m dbt.cli.main` works but prints a RuntimeWarning. Use the installed console script directly: `/home/airflow/.local/bin/dbt`.

### Inside-container vs host-port URLs

`docker-compose.yml` maps `8090:8080` (host:container). From your browser, the Airflow UI lives at `localhost:8090`. From *inside* another container on the same network, the api-server is `http://airflow-webserver:8080` — the host port is irrelevant for container-to-container traffic.

### PubChem rate limit can present as ECONNREFUSED, not 503

PubChem PUG REST allows max 5 req/s per IP. If you exceed it they sometimes drop the TCP connection rather than returning a clean 5xx. `pubchem_fetch.py` throttles synonym calls with `time.sleep(REQUEST_INTERVAL_S)` and uses `httpx.Client(trust_env=False, ...)` to ignore any stray proxy env vars.

### Don't conflate "Airflow Variables" with shell env vars

`{{ var.value.foo }}` resolves an **Airflow Variable** (row in the metadata DB), not an env var. We pass `USDA_API_KEY` directly via `.env` → docker-compose → container env → `os.getenv` in the fetch script; no Airflow Variable involved. The Airflow Variable system is only worth using when you want the value editable from the UI at runtime.

### Files that should never be committed

`data/ingredient.duckdb`, `dbt_project/.user.yml`, `dbt_project/target/`, `dbt_project/dbt_packages/`, `dbt_project/logs/`, `.env`. The first two are currently tracked and should be removed (`git rm --cached`).
