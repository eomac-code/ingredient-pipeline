"""
PubChem BigQuery loader.
Defines the raw_compounds schema and wires up the BigQueryLoader.
"""

from google.cloud import bigquery

from ingestion.bigquery_loader import BigQueryLoader

RAW_COMPOUNDS_SCHEMA = [
    bigquery.SchemaField("cid",                   "INTEGER"),
    bigquery.SchemaField("molecular_formula",     "STRING"),
    bigquery.SchemaField("molecular_weight",      "FLOAT"),
    bigquery.SchemaField("iupac_name",            "STRING"),
    bigquery.SchemaField("isomeric_smiles",       "STRING"),
    bigquery.SchemaField("inchi_key",             "STRING"),
    bigquery.SchemaField("xlogp",                 "FLOAT"),
    bigquery.SchemaField("hbond_donor_count",     "INTEGER"),
    bigquery.SchemaField("hbond_acceptor_count",  "INTEGER"),
    bigquery.SchemaField("charge",                "INTEGER"),
    bigquery.SchemaField("synonyms",              "STRING"),
    bigquery.SchemaField("_loaded_at",            "TIMESTAMP"),
]


def make_compounds_loader(project: str, dataset: str) -> BigQueryLoader:
    """Factory function — returns a configured loader for raw_compounds."""
    return BigQueryLoader(
        project=project,
        dataset=dataset,
        table="raw_compounds",
        schema=RAW_COMPOUNDS_SCHEMA,
    )