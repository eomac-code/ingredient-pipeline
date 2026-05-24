# ingredient-pipeline

A data pipeline that ingests food ingredient data from PubChem and USDA FoodData Central, transforms it with dbt, and serves a final ingredient catalog for flavor/nutrition R&D.

## Architecture

```
PubChem API          USDA FoodData Central API
     |                          |
pubchem_fetch.py         usda_fetch.py
     |                          |
raw_compounds (DuckDB)   raw_nutrition (DuckDB)
     |                          |
  stg_compounds           stg_nutrition       ← dbt staging (views)
          \                   /
        int_ingredient_enriched               ← dbt intermediate (ephemeral, fuzzy join)
                   |
          mart_ingredient_catalog             ← dbt mart (table)
```

**Orchestration:** Airflow DAG (`ingredient_pipeline_dag.py`) runs daily:
`ingest_pubchem → ingest_usda → dbt_seed → dbt_run → dbt_test`

## Key Files

| File | Purpose |
|------|---------|
| `pubchem_fetch.py` | Fetches compound properties + synonyms from PubChem PUG REST API → `raw_compounds` |
| `usda_fetch.py` | Fetches nutrient data from USDA FDC API → `raw_nutrition` |
| `ingredient_pipeline_dag.py` | Airflow DAG definition |
| `stg_compounds.sql` | Cleans/renames PubChem data |
| `stg_nutrition.sql` | Cleans/renames USDA data |
| `int_ingredient_enriched.sql` | Left-joins compounds + nutrition on fuzzy name match |
| `mart_ingredient_catalog.sql` | Final table with Lipinski + polarity classifications, FAIR metadata |
| `dbt_project.yml` | dbt config; staging=view, intermediate=ephemeral, marts=table |
| `profiles.yml` | dbt DuckDB connection (`data/ingredient.duckdb`) |
| `schema.yml` | dbt source + model docs and tests |
| `docker-compose.yml` | Airflow stack (Postgres + Redis + CeleryExecutor) |
| `ci.yml` | GitHub Actions: SQLFluff lint + dbt compile |
| `setup.sh` | One-command local setup |

## Local Setup

```bash
bash setup.sh
# Set USDA_API_KEY in .env (free key at fdc.nal.usda.gov)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PATH` | `data/ingredient.duckdb` | Path to DuckDB file |
| `USDA_API_KEY` | `DEMO_KEY` | USDA FDC API key (rate-limited without a real key) |

## Stack

- **Python 3.12**, httpx, tenacity, duckdb, dbt-duckdb 1.8
- **Airflow 2.9.2** (CeleryExecutor via Docker)
- **SQLFluff** for SQL linting, **pytest** for tests, **great-expectations** for data quality

## dbt Model Layers

- `staging/` — views; cast + rename raw sources, filter nulls
- `intermediate/` — ephemeral; fuzzy join on IUPAC name ↔ USDA search term
- `marts/` — table; adds Lipinski rule-of-five and polarity classifications, FAIR PubChem URLs
