"""
Abstract base class for all API clients.
Every new API source must implement fetch().
"""

from abc import ABC, abstractmethod


class BaseClient(ABC):

    @abstractmethod
    def fetch(self, *args, **kwargs) -> list[dict]:
        """Fetch raw records from the API and return them as a list of dicts."""