from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from app.schemas.analyticsExposureInternal import QueryResult


class QueryServiceInterface(ABC):
    """Interface for querying analytics data on behalf of the Analytics Exposure API."""

    @abstractmethod
    async def query(self,
        query: str,
        *,
        time: datetime | None = None,
        timeout: timedelta | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float | None = 30.0,
    ) -> QueryResult | None:
        pass

    @abstractmethod
    async def query_range(self,
        query: str,
        *,
        start: datetime,
        end: datetime,
        step: timedelta,
        timeout: timedelta | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float | None = 30.0,
    ) -> QueryResult | None:
        pass
