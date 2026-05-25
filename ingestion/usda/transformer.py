"""
USDA transformer.
Responsible ONLY for shaping raw API records into the schema
expected by the loader. No API calls, no DB calls — pure functions.
Easy to unit test.
"""

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


class USDATransformer:

    def transform(self, records: list[dict]) -> list[dict]:
        """Transform a list of raw USDA API records into loader-ready dicts."""
        return [self._transform_record(rec) for rec in records]

    def _transform_record(self, food: dict) -> dict:
        nutrients = self._extract_nutrients(food)
        return {
            "fdc_id":        food.get("fdcId"),
            "description":   food.get("description"),
            "search_term":   food.get("search_term"),
            "data_type":     food.get("dataType"),
            **nutrients,
        }

    def _extract_nutrients(self, food: dict) -> dict:
        """Flatten the nutrient list into a flat dict keyed by NUTRIENT_MAP labels."""
        nutrients = {label: None for label in NUTRIENT_MAP.values()}
        for nutrient in food.get("foodNutrients", []):
            num = str(nutrient.get("nutrientNumber", ""))
            if num in NUTRIENT_MAP:
                nutrients[NUTRIENT_MAP[num]] = nutrient.get("value")
        return nutrients