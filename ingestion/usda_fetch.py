"""
USDA FoodData Central API ingestion.

Fetches nutrient data for food ingredients and loads them
into DuckDB as the raw_nutrition table.

Docs: https://fdc.nal.usda.gov/api-guide.html
API key: free at https://fdc.nal.usda.gov/api-key-signup.html
Set env var: USDA_API_KEY=your_key  (or uses DEMO_KEY for low-volume testing)
"""

import os
from pathlib import Path

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(os.getenv("DUCKDB_PATH", "data/ingredient.duckdb"))
API_KEY = os.getenv("USDA_API_KEY", "DEMO_KEY")
BASE_URL = "https://api.nal.usda.gov/fdc/v1"

# Key nutrients we care about (nutrient number → label)
NUTRIENT_MAP = {
    "1003": "protein_g",
    "1004": "fat_g",
    "1005": "carbohydrate_g",
    "1008": "energy_kcal",
    "2000": "sugars_g",
    "1079": "fiber_g",
    "1087": "calcium_mg",
    "1089": "iron_mg",
    "1162": "vitamin_c_mg",
}

# Search terms — food ingredients relevant to flavor/nutrition R&D
SEARCH_TERMS = [
    "vanilla extract",
    "citric acid",
    "caffeine",
    "capsicum pepper",
    "menthol",
    "ascorbic acid",
    "glucose",
    "sucrose",
    "linalool",
    "limonene",
]


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def search_food(term: str) -> dict | None:
    """Search for a food item and return the top branded/SR result."""
    url = f"{BASE_URL}/foods/search"
    params = {
        "api_key": API_KEY,
        "query": term,
        "dataType": "Foundation,SR Legacy",
        "pageSize": 1,
    }

    with httpx.Client(timeout=30) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    foods = response.json().get("foods", [])
    return foods[0] if foods else None


def extract_nutrients(food: dict) -> dict:
    """Flatten nutrient list into a dict keyed by our NUTRIENT_MAP labels."""
    nutrients = {label: None for label in NUTRIENT_MAP.values()}

    for nutrient in food.get("foodNutrients", []):
        num = str(nutrient.get("nutrientNumber", ""))
        if num in NUTRIENT_MAP:
            nutrients[NUTRIENT_MAP[num]] = nutrient.get("value")

    return nutrients


# ---------------------------------------------------------------------------
# Load into DuckDB
# ---------------------------------------------------------------------------
def load_to_duckdb(records: list[dict]) -> None:
    """Create raw_nutrition table and insert records."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_nutrition (
            fdc_id          INTEGER,
            description     VARCHAR,
            search_term     VARCHAR,
            data_type       VARCHAR,
            protein_g       DOUBLE,
            fat_g           DOUBLE,
            carbohydrate_g  DOUBLE,
            energy_kcal     DOUBLE,
            sugars_g        DOUBLE,
            fiber_g         DOUBLE,
            calcium_mg      DOUBLE,
            iron_mg         DOUBLE,
            vitamin_c_mg    DOUBLE,
            _loaded_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("DELETE FROM raw_nutrition")

    for rec in records:
        con.execute("""
            INSERT INTO raw_nutrition VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp
            )
        """, [
            rec["fdc_id"],
            rec["description"],
            rec["search_term"],
            rec["data_type"],
            rec["protein_g"],
            rec["fat_g"],
            rec["carbohydrate_g"],
            rec["energy_kcal"],
            rec["sugars_g"],
            rec["fiber_g"],
            rec["calcium_mg"],
            rec["iron_mg"],
            rec["vitamin_c_mg"],
        ])

    count = con.execute("SELECT COUNT(*) FROM raw_nutrition").fetchone()[0]
    con.close()
    print(f"[usda] loaded {count} foods into raw_nutrition")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run() -> None:
    print(f"[usda] searching {len(SEARCH_TERMS)} ingredients...")
    records = []

    for term in SEARCH_TERMS:
        food = search_food(term)
        if not food:
            print(f"  [skip] no result for: {term}")
            continue

        nutrients = extract_nutrients(food)
        record = {
            "fdc_id": food.get("fdcId"),
            "description": food.get("description"),
            "search_term": term,
            "data_type": food.get("dataType"),
            **nutrients,
        }
        records.append(record)
        print(f"  [ok] {term} → {food.get('description')} (fdc_id={food.get('fdcId')})")

    load_to_duckdb(records)
    print("[usda] done.")


if __name__ == "__main__":
    run()
