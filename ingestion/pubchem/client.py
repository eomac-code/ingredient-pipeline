"""
PubChem PUG REST API client.
Responsible ONLY for fetching data from the API.

Docs: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest
"""

import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from ingestion.base_client import BaseClient
from ingestion.config import PUBCHEM_REQUEST_INTERVAL_S as REQUEST_INTERVAL_S, PUBCHEM_BASE_URL

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

class PubChemClient(BaseClient):

    def fetch(self, cids: list[int]) -> list[dict]:
        """
        Fetch properties + synonyms for a list of CIDs.
        Returns enriched records ready for transformation.
        """
        print(f"[pubchem] fetching properties for {len(cids)} compounds...")
        records = self._fetch_properties(cids)

        print("[pubchem] fetching synonyms...")
        for record in records:
            cid = record["CID"]
            record["synonyms"] = self._fetch_synonyms(cid)
            print(f"  cid={cid} synonyms={record['synonyms'][:2]}")
            time.sleep(REQUEST_INTERVAL_S)

        return records

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _fetch_properties(self, cids: list[int]) -> list[dict]:
        """Fetch compound properties for a batch of CIDs."""
        cid_str = ",".join(str(c) for c in cids)
        prop_str = ",".join(PROPERTIES)
        url = f"{BASE_URL}/compound/cid/{cid_str}/property/{prop_str}/JSON"

        with httpx.Client(timeout=30, trust_env=False) as client:
            response = client.get(url)
            response.raise_for_status()

        return response.json().get("PropertyTable", {}).get("Properties", [])

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _fetch_synonyms(self, cid: int) -> list[str]:
        """Fetch top 5 synonyms for a single CID."""
        url = f"{BASE_URL}/compound/cid/{cid}/synonyms/JSON"

        with httpx.Client(timeout=30, trust_env=False) as client:
            response = client.get(url)
            if response.status_code == 404:
                return []
            response.raise_for_status()

        info = response.json().get("InformationList", {}).get("Information", [{}])[0]
        return info.get("Synonym", [])[:5]