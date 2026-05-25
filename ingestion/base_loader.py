"""
Abstract base class for all data loaders.
Any new destination (BigQuery, Snowflake, etc.) must implement this interface.
"""

from abc import ABC, abstractmethod
class BaseLoader(ABC):

    @abstractmethod
    def create_table(self) -> None:
        """Create the destination table if it doesn't exist."""

    @abstractmethod
    def truncate(self) -> None:
        """Clear existing data (for idempotent loads)."""

    @abstractmethod
    def insert(self, records: list[dict]) -> None:
        """Insert records into the destination table."""

    def load(self, records: list[dict]) -> None:
        """Full load: create → truncate → insert. Call this from run()."""
        self.create_table()
        self.truncate()
        self.insert(records)