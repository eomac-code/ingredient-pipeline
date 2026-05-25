"""
Reusable BigQuery loader.
Pass in a table_id and schema and it handles everything else.
Used by both PubChem and USDA loaders.

Uses WRITE_APPEND load jobs to keep full history in raw tables.
Deduplication happens in dbt staging models using QUALIFY + ROW_NUMBER.
"""

import datetime
from google.cloud import bigquery
from ingestion.base_loader import BaseLoader
class BigQueryLoader(BaseLoader):

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        schema: list[bigquery.SchemaField],
    ) -> None:
        self.client = bigquery.Client(project=project)
        self.table_id = f"{project}.{dataset}.{table}"
        self.schema = schema

    def create_table(self) -> None:
        """Create table in BigQuery if it doesn't already exist."""
        try:
            self.client.get_table(self.table_id)
            print(f"[bigquery] table {self.table_id} already exists")
        except Exception:
            table = bigquery.Table(self.table_id, schema=self.schema)
            self.client.create_table(table)
            print(f"[bigquery] created table {self.table_id}")

    def truncate(self) -> None:
        """No-op — BigQuery uses WRITE_APPEND; no truncation needed."""
        pass

    def insert(self, records: list[dict]) -> None:
        """Append rows via a BigQuery load job. Keeps full history."""
        now = datetime.datetime.utcnow().isoformat()
        for rec in records:
            rec.setdefault("_loaded_at", now)

        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = self.client.load_table_from_json(
            records,
            self.table_id,
            job_config=job_config,
        )
        load_job.result()

        if load_job.errors:
            raise RuntimeError(f"[bigquery] load job errors: {load_job.errors}")

        print(f"[bigquery] appended {len(records)} rows into {self.table_id}")

    def load(self, records: list[dict]) -> None:
        """
        Override base load() to skip truncate entirely.
        create_table() → insert() only.
        """
        self.create_table()
        self.insert(records)