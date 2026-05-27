"""
Main entry point for the ingredient pipeline ingestion.
Orchestrates API fetching → transformation → loading into BigQuery.

Usage:
    python run.py
    python run.py --source pubchem
    python run.py --source usda
"""

import argparse
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

from ingestion.pubchem.client import PubChemClient
from ingestion.pubchem.transformer import PubChemTransformer
from ingestion.pubchem.loader import make_compounds_loader

from ingestion.usda.client import USDAClient
from ingestion.usda.transformer import USDATransformer
from ingestion.usda.loader import make_nutrition_loader
from ingestion.config import PUBCHEM_CIDS, USDA_SEARCH_TERMS

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all from environment variables
# ---------------------------------------------------------------------------
def _load_config() -> dict:
    """Load and validate required environment variables upfront."""
    required = ["DBT_GCP_PROJECT", "DBT_GCP_DATASET", "USDA_API_KEY"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")
    
    return {
        "project": os.environ["DBT_GCP_PROJECT"],
        "dataset": os.environ["DBT_GCP_DATASET"],
        "usda_api_key": os.environ["USDA_API_KEY"],
    }

# ---------------------------------------------------------------------------
# Pipeline runners
# ---------------------------------------------------------------------------
def run_pubchem(config: dict) -> tuple[bool, list[dict]]:
    """
    Run the PubChem pipeline.
    Returns True on success, False on failure.
    """
    log.info("=== PubChem Pipeline starting ===")

    try:
        client      = PubChemClient()
        transformer = PubChemTransformer()
        loader      = make_compounds_loader(config["project"], config["dataset"])

        raw = client.fetch(PUBCHEM_CIDS)
        records = transformer.transform(raw)
        log.info(f"[pubchem] transformed into {len(records)} records")

        loader.load(records)
        return True, records

    except EnvironmentError as e:
        log.error(f"[pubchem] configuration error: {e}")
        return False, []     
    except ConnectionError as e:
        log.error(f"[pubchem] network error during fetch: {e}")
        return False, []
    except Exception as e:
        log.exception(f"[pubchem] unexpected error: {e}")
        return False, []


def _extract_search_terms(pubchem_records: list[dict]) -> list[str]:
    """
    Derive USDA search terms from PubChem transformed records.
    Uses synonyms first, falls back to IUPACName.
    """
    terms = []
    for record in pubchem_records:
        synonyms = record.get("synonyms", [])
        if synonyms:
            terms.append(synonyms[0])   
        elif iupac := record.get("iupac_name"):
            terms.append(iupac)
    
    log.info(f"[linking] derived {len(terms)} USDA search terms from PubChem synonyms")
    return terms

def run_usda(config: dict, search_terms: list[str]) -> tuple[bool, list[dict]]:
    """
    Run the USDA pipeline.
    Returns True on success, False on failure.
    """
    log.info("=== USDA Pipeline starting ===")
    try:
        client      = USDAClient(api_key=config["usda_api_key"])
        transformer = USDATransformer()
        loader      = make_nutrition_loader(config["project"], config["dataset"])

        raw     = client.fetch(search_terms)
        records = transformer.transform(raw)
        log.info(f"[usda] transformed into {len(records)} records")
        loader.load(records)
        return True, records

    except EnvironmentError as e:
        log.error(f"[usda] configuration error: {e}")
        return False
    except ConnectionError as e:
        log.error(f"[usda] network error during fetch: {e}")
        return False, []
    except Exception as e:
        log.exception(f"[usda] unexpected error: {e}")
        return False, []


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

    try:
        config = _load_config()
    except EnvironmentError as e:
        log.critical(f"Startup failed: {e}")
        sys.exit(1)

    results = {}
    pubchem_records = []

    if args.source in ("pubchem", "all"):
        results["pubchem"], pubchem_records = run_pubchem(config)

    if args.source in ("usda", "all"):
        search_terms = (
            _extract_search_terms(pubchem_records)
            if pubchem_records
            else USDA_SEARCH_TERMS
        )
        results["usda"] = run_usda(config, search_terms)

    # Summary
    failed = [name for name, ok in results.items() if not ok]
    if failed:
        log.error(f"Pipeline finished with failures: {', '.join(failed)}")
        sys.exit(1)
    else:
        log.info("All pipelines completed successfully.")


if __name__ == "__main__":
    main()