"""
Main entry point for the ingredient pipeline ingestion.
Orchestrates API fetching → transformation → loading into BigQuery.

Usage:
    python run.py
    python run.py --source pubchem
    python run.py --source usda
"""

import argparse
import os
from dotenv import load_dotenv

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

from ingestion.pubchem.client import PubChemClient
from ingestion.pubchem.transformer import PubChemTransformer
from ingestion.pubchem.loader import make_compounds_loader

from ingestion.usda.client import USDAClient
from ingestion.usda.transformer import USDATransformer
from ingestion.usda.loader import make_nutrition_loader
from ingestion.config import PUBCHEM_CIDS, USDA_SEARCH_TERMS

# ---------------------------------------------------------------------------
# Config — all from environment variables
# ---------------------------------------------------------------------------
DBT_GCP_PROJECT = os.environ["DBT_GCP_PROJECT"]
DBT_GCP_DATASET = os.environ["DBT_GCP_DATASET"]
USDA_API_KEY = os.environ["USDA_API_KEY"]
GOOGLE_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# ---------------------------------------------------------------------------
# Pipeline runners
# ---------------------------------------------------------------------------
def run_pubchem() -> None:
    print("\n=== PubChem Pipeline ===")
    client      = PubChemClient()
    transformer = PubChemTransformer()
    loader      = make_compounds_loader(DBT_GCP_PROJECT, DBT_GCP_DATASET)

    raw     = client.fetch(PUBCHEM_CIDS)
    records = transformer.transform(raw)
    loader.load(records)
    print("[pubchem] done.")


def run_usda() -> None:
    print("\n=== USDA Pipeline ===")
    client      = USDAClient(api_key=USDA_API_KEY)
    transformer = USDATransformer()
    loader      = make_nutrition_loader(DBT_GCP_PROJECT, DBT_GCP_DATASET)

    raw     = client.fetch(USDA_SEARCH_TERMS)
    records = transformer.transform(raw)
    loader.load(records)
    print("[usda] done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingredient pipeline ingestion")
    parser.add_argument(
        "--source",
        choices=["pubchem", "usda", "all"],
        default="all",
        help="Which source to run (default: all)",
    )
    args = parser.parse_args()

    if args.source in ("pubchem", "all"):
        run_pubchem()

    if args.source in ("usda", "all"):
        run_usda()


if __name__ == "__main__":
    main()