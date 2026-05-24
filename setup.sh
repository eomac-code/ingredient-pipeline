#!/usr/bin/env bash
# scripts/setup.sh
# One-command local setup for ingredient-pipeline.
# Run from the project root: bash scripts/setup.sh

set -euo pipefail

echo "==> Setting up ingredient-pipeline"

# 1. python venv
echo "--> Creating virtual environment..."
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "    done."

# 2. env file
if [ ! -f .env ]; then
  cp .env.example .env
  echo "--> Created .env from .env.example — add your USDA_API_KEY"
fi

# 3. data directory
mkdir -p data
echo "--> data/ directory ready."

# 4. ingest
echo "--> Running ingestion scripts..."
python ingestion/pubchem_fetch.py
python ingestion/usda_fetch.py

# 5. dbt
echo "--> Running dbt..."
cd dbt_project
dbt deps
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
cd ..

echo ""
echo "==> Setup complete."
echo ""
echo "  View dbt docs:    cd dbt_project && dbt docs serve --profiles-dir ."
echo "  Start Airflow:    docker compose up airflow-init && docker compose up -d"
echo "  Open Airflow UI:  http://localhost:8080  (admin / admin)"
