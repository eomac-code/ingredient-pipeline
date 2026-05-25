.PHONY: help install activate deactivate ingest ingest-pubchem ingest-usda \
        dbt-run dbt-test dbt-compile dbt-docs test lint \
        airflow-up airflow-down airflow-logs airflow-restart clean

# ─── Settings ────────────────────────────────────────────────────────────────
PYTHON     := python3
VENV       := .venv
VENV_BIN   := $(VENV)/bin
DBT_DIR    := dbt_project

# ─── Help ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ingredient-pipeline — available commands"
	@echo ""
	@echo "  Environment"
	@echo "    make install       create .venv and install dependencies"
	@echo "    make activate      print the command to activate .venv"
	@echo "    make deactivate    print the command to deactivate .venv"
	@echo ""
	@echo "  Ingestion"
	@echo "    make ingest        run all ingestion sources → BigQuery"
	@echo "    make ingest-pubchem  run PubChem ingestion only"
	@echo "    make ingest-usda   run USDA ingestion only"
	@echo ""
	@echo "  dbt"
	@echo "    make dbt-run       build all dbt models"
	@echo "    make dbt-test      run all dbt tests"
	@echo "    make dbt-compile   compile dbt models (no warehouse needed)"
	@echo "    make dbt-docs      generate and serve dbt docs at localhost:8080"
	@echo ""
	@echo "  Testing & Linting"
	@echo "    make test          run pytest unit tests"
	@echo "    make lint          run SQLFluff lint on dbt models"
	@echo ""
	@echo "  Airflow"
	@echo "    make airflow-up    start Airflow stack"
	@echo "    make airflow-down  stop Airflow stack"
	@echo "    make airflow-logs  tail Airflow scheduler logs"
	@echo "    make airflow-restart  restart all Airflow services"
	@echo ""
	@echo "  Housekeeping"
	@echo "    make clean         remove compiled artifacts and caches"
	@echo ""

# ─── Environment ─────────────────────────────────────────────────────────────
install:
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements.txt
	@echo ""
	@echo "  ✓ .venv created. Run: source .venv/bin/activate"

activate:
	@echo ""
	@echo "  Run this command to activate your virtual environment:"
	@echo ""
	@echo "    source .venv/bin/activate"
	@echo ""
	@echo "  (make cannot activate a shell for you — it runs in a subprocess)"

deactivate:
	@echo ""
	@echo "  Run this command to deactivate your virtual environment:"
	@echo ""
	@echo "    deactivate"
	@echo ""

# ─── Ingestion ───────────────────────────────────────────────────────────────
ingest:
	$(PYTHON) run.py

ingest-pubchem:
	$(PYTHON) run.py --source pubchem

ingest-usda:
	$(PYTHON) run.py --source usda

# ─── dbt ─────────────────────────────────────────────────────────────────────
dbt-run:
	cd $(DBT_DIR) && dbt run --profiles-dir .

dbt-test:
	cd $(DBT_DIR) && dbt test --profiles-dir .

dbt-compile:
	cd $(DBT_DIR) && dbt compile --profiles-dir .

dbt-docs:
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ─── Testing & Linting ───────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v

lint:
	cd $(DBT_DIR) && sqlfluff lint models

lint-fix:
	cd $(DBT_DIR) && sqlfluff fix models

# ─── Airflow ─────────────────────────────────────────────────────────────────
airflow-up:
	docker compose up -d
	@echo ""
	@echo "  ✓ Airflow running at http://localhost:8090"

airflow-down:
	docker compose down

airflow-logs:
	docker compose logs -f airflow-scheduler

airflow-restart:
	docker compose down
	docker compose up -d
	@echo ""
	@echo "  ✓ Airflow restarted at http://localhost:8090"

# ─── Housekeeping ────────────────────────────────────────────────────────────
clean:
	rm -rf $(DBT_DIR)/target/
	rm -rf $(DBT_DIR)/dbt_packages/
	rm -rf $(DBT_DIR)/logs/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "  ✓ cleaned build artifacts and caches"