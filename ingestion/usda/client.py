"""
USDA FoodData Central API client.
Responsible ONLY for fetching data from the API.

Docs: https://fdc.nal.usda.gov/api-guide.html
"""

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from ingestion.base_client import BaseClient
from ingestion.config import USDA_BASE_URL

class USDAClient(BaseClient):

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch(self, search_terms: list[str]) -> list[dict]:
        """
        Search for each term and return raw API results.
        Returns list of dicts with food data + search_term attached.
        """
        results = []
        for term in search_terms:
            food = self._search_food(term)
            if not food:
                print(f"  [skip] no result for: {term}")
                continue
            food["search_term"] = term   # attach term for traceability
            results.append(food)
            print(f"  [ok] {term} → {food.get('description')} (fdc_id={food.get('fdcId')})")
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _search_food(self, term: str) -> dict | None:
        """Search for a food item and return the top Foundation/SR result."""
        url = f"{USDA_BASE_URL}/foods/search"
        params = {
            "api_key": self.api_key,
            "query": term,
            "dataType": "Foundation,SR Legacy",
            "pageSize": 1,
        }

        with httpx.Client(timeout=30) as client:
            response = client.get(url, params=params)
            response.raise_for_status()

        foods = response.json().get("foods", [])
        return foods[0] if foods else None