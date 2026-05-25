# ingredient-pipeline

End-to-end data pipeline that ingests public chemical and nutritional ingredient data, transforms it through dbt layers, validates quality, and exposes a clean analytical dataset — designed to mirror FAIR-compliant R&D data engineering practices.

Built as a portfolio project for a Data Engineer role in a Research, Innovation & Labs context.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────────────┐
│  PubChem    │────►│ PubChemClient│────►│                   │
│  API        │     │ + Transformer│     │  BigQuery         │
└─────────────┘     └──────────────┘     │                   │
                                         │  raw_compounds    │ ← appends every run
┌─────────────┐     ┌──────────────┐     │  raw_nutrition    │ ← appends every run
│  USDA       │────►│ USDAClient   │────►│                   │
│  API        │     │ + Transformer│     └────────┬──────────┘
└─────────────┘     └──────────────┘              │
                                                  │ dbt run
                                         ┌────────▼──────────┐
                                         │  stg_compounds    │ ← deduplicated latest
                                         │  stg_nutrition    │ ← deduplicated latest
                                         └────────┬──────────┘
                                                  │ dbt run
                                         ┌────────▼──────────┐
                                         │  marts            │ ← research-ready
                                         └───────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + httpx + tenacity | Fetch from PubChem & USDA APIs with retry logic |
| Warehouse | Google BigQuery | Cloud analytical warehouse |
| Transformation | dbt Core + dbt-bigquery | Staging → Intermediate → Mart models |
| Data Quality | dbt tests | Schema tests, custom business rules |
| Orchestration | Apache Airflow (Docker) | Linear DAG: ingest → transform → test |
| Containerisation | Docker Compose | Airflow stack (scheduler, webserver, worker, postgres, redis) |

---

## FAIR Data Principles

| Principle | Implementation |
|---|---|
| **Findable** | All datasets documented in `schema.yml` with descriptions and tags |
| **Accessible** | Source data from open public APIs (PubChem, USDA) with documented endpoints |
| **Interoperable** | Standard column naming conventions, SMILES notation preserved, SI units enforced |
| **Reusable** | dbt model lineage documented, tests cover all key fields, `_loaded_at` timestamp on every raw row |

---

## Data Strategy

Raw tables use **append-only loads** (`WRITE_APPEND`) so every pipeline run adds a new timestamped snapshot. This means:

- Full history is always preserved in `raw_compounds` and `raw_nutrition`
- Researchers can query how values changed over time
- dbt staging models deduplicate using `QUALIFY ROW_NUMBER()` to always surface the latest record

```sql
-- How has caffeine's molecular weight changed across runs?
select _loaded_at, molecular_weight
from `ingredients.raw_compounds`
where cid = 2519
order by _loaded_at
```

---

## Project Structure

```
ingredient-pipeline/
├── run.py                              # single entry point for all ingestion
├── requirements.txt
├── docker-compose.yml
├── .env                                # DBT_GCP_PROJECT, DBT_GCP_DATASET, USDA_API_KEY, GOOGLE_APPLICATION_CREDENTIALS
│
├── ingestion/
│   ├── base_client.py                  # abstract API client interface
│   ├── base_loader.py                  # abstract loader interface
│   ├── bigquery_loader.py              # reusable BigQuery loader (WRITE_APPEND)
│   ├── pubchem/
│   │   ├── client.py                   # PubChem PUG REST API calls
│   │   ├── transformer.py              # shapes raw API response → clean records
│   │   └── loader.py                   # BigQuery schema for raw_compounds
│   └── usda/
│       ├── client.py                   # USDA FoodData Central API calls
│       ├── transformer.py              # shapes raw API response → clean records
│       └── loader.py                   # BigQuery schema for raw_nutrition
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_compounds.sql       # PubChem raw → typed + deduplicated
│   │   │   ├── stg_nutrition.sql       # USDA raw → typed + deduplicated
│   │   │   └── schema.yml
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
│
├── airflow/
│   └── dags/
│       └── ingredient_pipeline_dag.py  # ingest_pubchem → ingest_usda → dbt_run → dbt_test
│
└── tests/
    ├── test_pubchem_transformer.py
    ├── test_usda_transformer.py
    └── test_bigquery_loader.py
```

---

## Quick Start

### Prerequisites
- Python 3.12+
- Google Cloud project with BigQuery API enabled
- Service account key JSON with BigQuery Admin role
- Docker + Docker Compose

### 1 — Clone and set up environment
```bash
git clone https://github.com/<you>/ingredient-pipeline.git
cd ingredient-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2 — Configure environment variables
```bash
cp .env.example .env
```

Edit `.env`:
```
DBT_GCP_PROJECT=your-gcp-project-id
DBT_GCP_DATASET=ingredientes_dev
USDA_API_KEY=your_usda_key        # free at https://fdc.nal.usda.gov/api-key-signup.html
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/keyfile.json
```

### 3 — Run ingestion locally
```bash
# Run all sources
python run.py

# Or run individually
python run.py --source pubchem
python run.py --source usda
```

### 4 — Run dbt
```bash
cd dbt_project
dbt debug                                    # verify BigQuery connection
dbt run                                      # build all models
dbt test                                     # run all tests
dbt docs generate && dbt docs serve          # browse lineage at localhost:8080
```

### 5 — Run tests
```bash
pytest tests/
```

### 6 — Start Airflow (optional)
```bash
docker compose down 
docker compose build
docker compose up airflow-init
docker compose up -d
open http://localhost:8090        # admin / your AIRFLOW_ADMIN_PASSWORD
```

---

## dbt Model Lineage

```
raw_compounds ──► stg_compounds ──┐
                                  ├──► int_ingredient_enriched ──► mart_ingredient_catalog
raw_nutrition ──► stg_nutrition ──┘
```

---

## Ingestion Design

Each source follows the same three-class pattern making it easy to add new sources:

| Class | Responsibility |
|---|---|
| `Client` | Talk to the API only — no transformation, no DB |
| `Transformer` | Shape raw API response into clean records — no API, no DB |
| `Loader` | Define BigQuery schema and load records — no API logic |

To add a new source (e.g. OpenFoodFacts):
1. Create `ingestion/openfoodfacts/client.py`
2. Create `ingestion/openfoodfacts/transformer.py`
3. Create `ingestion/openfoodfacts/loader.py`
4. Add `run_openfoodfacts()` to `run.py`
5. Add a task to the Airflow DAG