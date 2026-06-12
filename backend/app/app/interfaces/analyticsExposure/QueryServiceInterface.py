from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from app.interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface
from app.schemas.analyticsExposureInternal import QueryResult


class QueryServiceInterface(ABC):
    """Interface for querying analytics data on behalf of the Analytics Exposure API."""

    @abstractmethod
    async def query(self,
        builder: QueryBuilderInterface,
        *,
        time: datetime,
        timeout: timedelta | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float | None = 30.0,
    ) -> QueryResult | None:
        pass

    @abstractmethod
    async def query_range(self,
        builder: QueryBuilderInterface,
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
