from typing import Self, Iterable
from abc import ABC, abstractmethod
from app.interfaces.analyticsExposure.Query import Query, QueryArgs


class QueryBuilderInterface(ABC):
    """Interface for building queries for querying analytics data on behalf of the Analytics Exposure API."""

    @abstractmethod
    def build_query(self, query: Query, args: QueryArgs) -> str:
        pass

    @abstractmethod
    def build_multi_query(self, queries: Iterable[str]) -> str:
        pass