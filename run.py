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

# ---------------------------------------------------------------------------
# Config — all from environment variables
# ---------------------------------------------------------------------------
GCP_PROJECT = os.environ["GCP_PROJECT"]
GCP_DATASET = os.environ["GCP_DATASET"]
USDA_API_KEY = os.environ["USDA_API_KEY"]
GOOGLE_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

print(f"[config] project      : {GCP_PROJECT}")
print(f"[config] dataset      : {GCP_DATASET}")

# Seed CIDs: vanillin, caffeine, citric acid, ascorbic acid, capsaicin,
# menthol, linalool, limonene, glucose, sucrose
PUBCHEM_CIDS = [8468, 2519, 311, 54670067, 1548943, 16666, 6549, 440917, 5793, 5988]

USDA_SEARCH_TERMS = [
    "vanilla extract", "citric acid", "caffeine", "capsicum pepper",
    "menthol", "ascorbic acid", "glucose", "sucrose", "linalool", "limonene",
]


# ---------------------------------------------------------------------------
# Pipeline runners
# ---------------------------------------------------------------------------
def run_pubchem() -> None:
    print("\n=== PubChem Pipeline ===")
    client      = PubChemClient()
    transformer = PubChemTransformer()
    loader      = make_compounds_loader(GCP_PROJECT, GCP_DATASET)

    raw     = client.fetch(PUBCHEM_CIDS)
    records = transformer.transform(raw)
    loader.load(records)
    print("[pubchem] done.")


def run_usda() -> None:
    print("\n=== USDA Pipeline ===")
    client      = USDAClient(api_key=USDA_API_KEY)
    transformer = USDATransformer()
    loader      = make_nutrition_loader(GCP_PROJECT, GCP_DATASET)

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