from abc import ABC, abstractmethod
from typing_extensions import Self

from app.interfaces.analyticsExposure.Query import Query, QueryArgs


class QueryBuilderInterface(ABC):
    """Interface for stateful builder for querying analytics data on behalf of the Analytics Exposure API."""

    @abstractmethod
    def add(self, query: Query, args: QueryArgs) -> Self:
        """Add a sub-query. Returns self to allow chaining."""
        pass

    @abstractmethod
    def build(self) -> str:
        """Produce the combined query string from all added sub-queries."""
        pass

    @property
    @abstractmethod
    def queries(self) -> dict[str, tuple[Query, QueryArgs]]:
        """Mapping of Query.type → (Query, QueryArgs) for each added sub-query."""
        pass

