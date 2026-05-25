"""
USDA BigQuery loader.
Defines the raw_nutrition schema and wires up the BigQueryLoader.
"""

from google.cloud import bigquery

from ingestion.bigquery_loader import BigQueryLoader

RAW_NUTRITION_SCHEMA = [
    bigquery.SchemaField("fdc_id",         "INTEGER"),
    bigquery.SchemaField("description",    "STRING"),
    bigquery.SchemaField("search_term",    "STRING"),
    bigquery.SchemaField("data_type",      "STRING"),
    bigquery.SchemaField("protein_g",      "FLOAT"),
    bigquery.SchemaField("fat_g",          "FLOAT"),
    bigquery.SchemaField("carbohydrate_g", "FLOAT"),
    bigquery.SchemaField("energy_kcal",    "FLOAT"),
    bigquery.SchemaField("sugars_g",       "FLOAT"),
    bigquery.SchemaField("fiber_g",        "FLOAT"),
    bigquery.SchemaField("calcium_mg",     "FLOAT"),
    bigquery.SchemaField("iron_mg",        "FLOAT"),
    bigquery.SchemaField("vitamin_c_mg",   "FLOAT"),
    bigquery.SchemaField("_loaded_at",     "TIMESTAMP"),
]


def make_nutrition_loader(project: str, dataset: str) -> BigQueryLoader:
    """Factory function — returns a configured loader for raw_nutrition."""
    return BigQueryLoader(
        project=project,
        dataset=dataset,
        table="raw_nutrition",
        schema=RAW_NUTRITION_SCHEMA,
    )