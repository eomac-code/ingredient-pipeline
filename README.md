# ingredient-pipeline

End-to-end data pipeline that ingests public chemical and nutritional ingredient data, transforms it through dbt layers, validates quality, and exposes a clean analytical dataset — designed to mirror FAIR-compliant R&D data engineering practices.

Built as a portfolio project for a Data Engineer role in a Research, Innovation & Labs context.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Airflow DAG                          │
│                  (ingredient_pipeline_dag)                  │
│                                                             │
│   [ingest_pubchem] → [ingest_usda] → [dbt_run] → [dbt_test]│
└─────────────────────────────────────────────────────────────┘
         │                   │               │
         ▼                   ▼               ▼
   PubChem REST API    USDA FoodData    DuckDB warehouse
   (compounds,         Central API     (local file)
    properties,        (nutrients,
    SMILES)            foods)

                            │
                   ┌────────▼────────┐
                   │   dbt layers    │
                   │                 │
                   │  staging/       │  ← raw → typed, renamed
                   │  intermediate/  │  ← joins, enrichment
                   │  marts/         │  ← final analytical models
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │  dbt tests +    │
                   │  data quality   │
                   │  (schema tests, │
                   │   custom tests) │
                   └─────────────────┘
```

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + httpx | Fetch from PubChem & USDA APIs |
| Warehouse | DuckDB | Local analytical database |
| Transformation | dbt Core + dbt-duckdb | Staging → Intermediate → Mart models |
| Data Quality | dbt tests + Great Expectations | Schema tests, custom business rules |
| Orchestration | Apache Airflow (Docker) | Linear DAG: ingest → transform → test |
| CI/CD | GitHub Actions | Lint + dbt compile on every push |
| Containerisation | Docker Compose | Airflow stack (scheduler, webserver, postgres, redis) |

## FAIR Data Principles

| Principle | Implementation |
|---|---|
| **Findable** | All datasets documented in `schema.yml` with descriptions and tags |
| **Accessible** | Source data from open public APIs (PubChem, USDA) with documented endpoints |
| **Interoperable** | Standard column naming conventions, SMILES notation preserved, SI units enforced |
| **Reusable** | dbt model lineage documented, tests cover all key fields, seeds for reference data |

## Project Structure

```
ingredient-pipeline/
├── ingestion/
│   ├── pubchem_fetch.py          # PubChem PUG REST API client
│   └── usda_fetch.py             # USDA FoodData Central API client
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_compounds.sql         # PubChem raw → typed
│   │   │   ├── stg_nutrition.sql         # USDA raw → typed
│   │   │   └── schema.yml                # source + model docs + tests
│   │   ├── intermediate/
│   │   │   ├── int_ingredient_enriched.sql  # join compounds + nutrition
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── mart_ingredient_catalog.sql  # final analytical model
│   │       └── schema.yml
│   ├── tests/
│   │   └── assert_molecular_weight_positive.sql
│   ├── seeds/
│   │   └── ingredient_categories.csv
│   ├── macros/
│   │   └── clean_string.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── airflow/
│   └── dags/
│       └── ingredient_pipeline_dag.py
├── quality/
│   └── expectations/
├── scripts/
│   └── setup.sh                  # one-command local setup
├── .github/
│   └── workflows/
│       └── ci.yml
├── docker-compose.yml            # Airflow stack
├── requirements.txt
├── .env.example
└── README.md
```

## Quick Start

```bash
# 1. clone and enter
git clone https://github.com/<you>/ingredient-pipeline.git
cd ingredient-pipeline

# 2. python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. ingest raw data into DuckDB
python ingestion/pubchem_fetch.py
python ingestion/usda_fetch.py

# 4. run dbt
cd dbt_project
dbt debug          # verify connection
dbt run            # build all models
dbt test           # run all tests
dbt docs generate && dbt docs serve   # browse lineage

# 5. start Airflow (optional)
cd ..
docker compose up airflow-init   # first time only
docker compose up -d
open http://localhost:8080        # admin / admin
```

## dbt Model Lineage

```
stg_compounds ──┐
                ├──► int_ingredient_enriched ──► mart_ingredient_catalog
stg_nutrition ──┘
```
