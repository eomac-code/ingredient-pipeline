"""
PubChem PUG REST API ingestion.

Fetches compound properties for a list of CIDs and loads them
into DuckDB as the raw_compounds table.

Docs: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest
"""

import json
import os
from pathlib import Path

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(os.getenv("DUCKDB_PATH", "data/ingredient.duckdb"))
BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

# Properties to fetch per compound
PROPERTIES = [
    "MolecularFormula",
    "MolecularWeight",
    "IUPACName",
    "IsomericSMILES",
    "InChIKey",
    "XLogP",
    "HBondDonorCount",
    "HBondAcceptorCount",
    "Charge",
]

# Seed CIDs: common food-relevant compounds
# vanillin, caffeine, citric acid, ascorbic acid, capsaicin,
# menthol, linalool, limonene, glucose, sucrose
SEED_CIDS = [8468, 2519, 311, 54670067, 1548943, 16666, 6549, 440917, 5793, 5988]


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_compound_properties(cids: list[int]) -> list[dict]:
    """Fetch properties for a batch of CIDs from PubChem."""
    cid_str = ",".join(str(c) for c in cids)
    prop_str = ",".join(PROPERTIES)
    url = f"{BASE_URL}/compound/cid/{cid_str}/property/{prop_str}/JSON"

    with httpx.Client(timeout=30) as client:
        response = client.get(url)
        response.raise_for_status()

    data = response.json()
    return data.get("PropertyTable", {}).get("Properties", [])


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_compound_synonyms(cid: int) -> list[str]:
    """Fetch top synonyms (common names) for a single CID."""
    url = f"{BASE_URL}/compound/cid/{cid}/synonyms/JSON"

    with httpx.Client(timeout=30) as client:
        response = client.get(url)
        if response.status_code == 404:
            return []
        response.raise_for_status()

    data = response.json()
    synonyms = data.get("InformationList", {}).get("Information", [{}])[0]
    return synonyms.get("Synonym", [])[:5]  # top 5 only


# ---------------------------------------------------------------------------
# Load into DuckDB
# ---------------------------------------------------------------------------
def load_to_duckdb(records: list[dict]) -> None:
    """Create raw_compounds table and insert records."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_compounds (
            cid                   INTEGER,
            molecular_formula     VARCHAR,
            molecular_weight      DOUBLE,
            iupac_name            VARCHAR,
            isomeric_smiles       VARCHAR,
            inchi_key             VARCHAR,
            xlogp                 DOUBLE,
            hbond_donor_count     INTEGER,
            hbond_acceptor_count  INTEGER,
            charge                INTEGER,
            synonyms              VARCHAR,
            _loaded_at            TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # clear and reload (idempotent)
    con.execute("DELETE FROM raw_compounds")

    for rec in records:
        con.execute("""
            INSERT INTO raw_compounds VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [
            rec.get("CID"),
            rec.get("MolecularFormula"),
            rec.get("MolecularWeight"),
            rec.get("IUPACName"),
            rec.get("IsomericSMILES"),
            rec.get("InChIKey"),
            rec.get("XLogP"),
            rec.get("HBondDonorCount"),
            rec.get("HBondAcceptorCount"),
            rec.get("Charge"),
            json.dumps(rec.get("synonyms", [])),
        ])

    count = con.execute("SELECT COUNT(*) FROM raw_compounds").fetchone()[0]
    con.close()
    print(f"[pubchem] loaded {count} compounds into raw_compounds")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run() -> None:
    print(f"[pubchem] fetching {len(SEED_CIDS)} compounds...")
    properties = fetch_compound_properties(SEED_CIDS)

    # enrich with synonyms
    print("[pubchem] fetching synonyms...")
    for compound in properties:
        cid = compound["CID"]
        compound["synonyms"] = fetch_compound_synonyms(cid)
        print(f"  cid={cid} synonyms={compound['synonyms'][:2]}")

    load_to_duckdb(properties)
    print("[pubchem] done.")


if __name__ == "__main__":
    run()
