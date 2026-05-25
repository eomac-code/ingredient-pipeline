# ingredient-pipeline

A data pipeline that ingests food ingredient data from PubChem and USDA FoodData Central, transforms it with dbt, and serves a final ingredient catalog for flavor/nutrition R&D.

## Architecture

```
PubChem API                    USDA FoodData Central API
      |                                   |
ingestion/pubchem/             ingestion/usda/
  client.py                      client.py
  transformer.py                 transformer.py
  loader.py                      loader.py
      |                                   |
      └──────────── run.py ───────────────┘
                       |
         BigQuery (dataset: ingredientes_dev)
                       |
          raw_compounds / raw_nutrition        ← WRITE_APPEND every run
                       |
              dbt staging models
          stg_compounds / stg_nutrition        ← deduplicated views (QUALIFY ROW_NUMBER)
                       |
          int_ingredient_enriched              ← ephemeral, fuzzy join
                       |
          mart_ingredient_catalog              ← final research table
```

**Orchestration:** Airflow DAG (`ingredient_pipeline_dag.py`) runs daily, CeleryExecutor:
`ingest_pubchem → ingest_usda → dbt_run → dbt_test`

`max_active_runs=1` to prevent overlapping BigQuery writes.

## Stack

- **Python 3.12**, httpx>=0.28.1, tenacity, google-cloud-bigquery, python-dotenv
- **dbt-core 1.11**, dbt-bigquery 1.8
- **Airflow** (CeleryExecutor via Docker: api-server, scheduler, worker, triggerer, postgres, redis)
- **Google BigQuery** — cloud analytical warehouse (replaces DuckDB)
- SQLFluff (jinja templater), pytest

## Ingestion design

Each source follows a three-class pattern making it easy to add new sources:

| Class | Responsibility |
|---|---|
| `client.py` | API calls only — no transformation, no DB |
| `transformer.py` | Shape raw API response into clean records — pure functions, fully testable |
| `loader.py` | Define BigQuery schema and wire up `BigQueryLoader` |

Base classes in `ingestion/base_client.py` and `ingestion/base_loader.py` enforce the interface.
`BigQueryLoader` is reusable across all sources — pass in a table name and schema, it handles the rest.

### Entry point

```bash
python run.py                  # run all sources
python run.py --source pubchem # run one source
python run.py --source usda
```

`run.py` calls `load_dotenv()` first, then orchestrates client → transformer → loader for each source.

## Data strategy — append + deduplicate

Raw tables use **WRITE_APPEND** load jobs so every pipeline run adds a new timestamped snapshot:

```
raw_compounds     → grows every run (_loaded_at timestamp on every row)
stg_compounds     → QUALIFY ROW_NUMBER() OVER (PARTITION BY cid ORDER BY _loaded_at DESC) = 1
```

Researchers get two query surfaces:
- **Staging/marts** — always current, deduplicated
- **Raw tables** — full history, track how values changed over time

```sql
-- How has caffeine's molecular weight changed across runs?
SELECT _loaded_at, molecular_weight
FROM `ingredients.raw_compounds`
WHERE cid = 2519
ORDER BY _loaded_at
```

## dbt model layers

| Layer | Materialisation | Schema | Purpose |
|---|---|---|---|
| `staging/` | view | `ingredientes_dev` | Cast + rename raw sources, deduplicate via QUALIFY |
| `intermediate/` | ephemeral | — | Fuzzy join IUPAC name ↔ USDA search term. Inlined into mart. |
| `marts/` | table | `ingredientes_dev` | Lipinski rule-of-five, polarity class, FAIR PubChem URLs, data-quality flags |

### Column naming convention (staging → downstream)

| Raw column | Staging alias | Why |
|---|---|---|
| `cid` | `compound_id` | Generic, warehouse-agnostic name |
| `isomeric_smiles` | `smiles` | Shorter, standard cheminformatics term |
| `molecular_weight` | `molecular_weight_g_mol` | Explicit unit in name |
| `charge` | `formal_charge` | Matches IUPAC terminology |
| `synonyms` | `synonyms_json` | Signals JSON-encoded string |
| `_loaded_at` | `ingested_at` | Business-friendly name |
| `fdc_id` | `food_id` | Generic surrogate key |
| `description` | `food_description` | Disambiguates from other description columns |

## Container paths (apache/airflow:3.2.1-python3.12)

| Thing | Path |
|---|---|
| Project root (host bind-mounted) | `/opt/airflow/project` |
| Python interpreter | `/usr/local/bin/python3` |
| dbt CLI | `/home/airflow/.local/bin/dbt` |
| GCP keyfile (read-only mount) | `/opt/airflow/secrets/gcp_keyfile.json` |

## Environment variables

| Variable | Where set | Description |
|---|---|---|
| `GCP_PROJECT` | `.env` → docker-compose → container | GCP project ID |
| `GCP_DATASET` | `.env` → docker-compose → container | BigQuery dataset name |
| `GOOGLE_APPLICATION_CREDENTIALS` | `.env` → docker-compose → mounted as file | Path to service account keyfile |
| `USDA_API_KEY` | `.env` → docker-compose → container | USDA FDC API key |
| `AIRFLOW_JWT_SECRET` | `.env` → docker-compose | Shared JWT signing secret for Airflow 3.x Task SDK |
| `AIRFLOW_ADMIN_PASSWORD` | `.env` → docker-compose | Simple-auth-manager admin password |
| `AIRFLOW_UID` | `.env` | UID the airflow containers run as |

## Local setup

```bash
# 1. clone and set up environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. configure .env
cp .env.example .env
# fill in: GCP_PROJECT, GCP_DATASET, USDA_API_KEY, GOOGLE_APPLICATION_CREDENTIALS
# fill in: AIRFLOW_JWT_SECRET, AIRFLOW_ADMIN_PASSWORD, AIRFLOW_UID

# 3. run ingestion
python run.py

# 4. run dbt
cd dbt_project
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate && dbt docs serve

# 5. run tests
cd ..
pytest tests/

# 6. start Airflow
docker compose build
docker compose up airflow-init
docker compose up -d
open http://localhost:8090
```

## CI/CD (GitHub Actions)

Two jobs in `.github/workflows/ci.yml`:

| Job | Triggers | Steps |
|---|---|---|
| `lint-and-compile` | every PR + push | SQLFluff lint → dbt deps → dbt compile |
| `test` | push to main only | dbt test against real BigQuery |

GitHub configuration required:
- **Repository variables**: `GCP_PROJECT`, `GCP_DATASET`
- **Repository secret**: `GOOGLE_APPLICATION_CREDENTIALS` (full JSON contents of service account keyfile)

SQLFluff uses `jinja` templater (not `dbt`) to avoid profile loading issues in CI. Config lives in `dbt_project/.sqlfluff`.

## Files that should never be committed

```
.env
dbt_project/.user.yml
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
gcp_key/
```

Remove any accidentally tracked files:
```bash
git rm --cached dbt_project/.user.yml
git rm --cached -r dbt_project/target/
```

---

## Gotchas + lessons learned

### BigQuery streaming buffer vs DELETE

`insert_rows_json()` puts rows in a streaming buffer for up to 90 minutes. You cannot run `DELETE FROM table WHERE TRUE` while rows are buffered — BigQuery throws `invalidQuery`. Always use `load_table_from_json()` with `WRITE_APPEND` or `WRITE_TRUNCATE` instead.

### `dataset` vs `schema` in profiles.yml

`dbt-bigquery` maps BigQuery datasets to dbt's internal `schema` concept. In `profiles.yml` use `schema`, not `dataset` — even though BigQuery calls it a dataset:

```yaml
# correct
schema: "{{ env_var('GCP_DATASET') }}"

# wrong — causes "Got duplicate keys: (dataset) all map to schema"
dataset: "{{ env_var('GCP_DATASET') }}"
schema: "{{ env_var('GCP_DATASET') }}"
```

### Don't put sources and models in the same schema.yml

`sources:` blocks belong only in `sources.yml`. Defining sources in `schema.yml` as well creates duplicate source definitions and runs tests twice against raw tables — which fail because raw tables have duplicate rows by design (append-only).

### Raw tables have duplicate rows by design

Every pipeline run appends new rows. `unique` tests on raw tables will always fail. Only test `unique` on staging models after `QUALIFY` deduplication.

### GCP keyfile in GitHub Actions

`echo '${{ secrets.KEY }}'` with single quotes corrupts JSON special characters. Use:
```yaml
env:
  GCP_KEYFILE: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
run: echo "$GCP_KEYFILE" > /tmp/gcp_keyfile.json
```

### Airflow Variables vs shell env vars

`{{ var.value.foo }}` resolves an Airflow Variable (stored in metadata DB), not a shell env var. All config (`GCP_PROJECT`, `GCP_DATASET`, `USDA_API_KEY`) is passed via `.env` → docker-compose → container environment — no Airflow Variables needed.

### Airflow 3.x Task SDK

Workers communicate with the api-server via HTTPS, not Postgres. Two configs required:
1. `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-webserver:8080/execution/`
2. `AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}` — must be shared across all containers

If you see `httpx.ConnectError` from `airflow/sdk/api/client.py` the problem is Airflow internals, not your DAG.

### dbt invocation

`python -m dbt` does not work. Use the installed console script directly: `/home/airflow/.local/bin/dbt`.