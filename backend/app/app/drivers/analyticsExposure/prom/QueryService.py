import logging

import httpx
from datetime import datetime, timedelta

from pydantic.tools import parse_obj_as

from app.interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface
from app.interfaces.analyticsExposure.QueryServiceInterface import QueryServiceInterface
from app.schemas.analyticsExposureInternal import QueryResult

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


"""
NOTE ABOUT PROMETHEUS QUERIES
Prometheus omits results for queries where it has no data for the base metric. This means that if
no traffic was observed for a given UE or app in the queried time window, there will be no series
for that UE/app in the query result (rather than a series with a default value of 0). This is true
for all queries (even rate queries) if the underlying metric has no samples in the queried time window.

Callers must decide if they should return zeroed default values to the user and handle this explicitly:
- empty data.result list means no traffic for all apps/UEs in the whole timerange.
- missing entry for a specific UE/app in data.result means no traffic for that UE/app in the whole timerange.
- for the current catalog queries missing (ts, value) in matrix result means no traffic for that UE/app at that specific timestamp.
"""


class PromQueryService(QueryServiceInterface):

    def __init__(self, prometheus_url):
        self.__httpx_client = httpx.AsyncClient(
            base_url=prometheus_url,
            timeout=httpx.Timeout(20, connect=3.05, read=27)
        )

    async def query(self,
        builder: QueryBuilderInterface,
        *,
        time: datetime,
        timeout: timedelta | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float = httpx.USE_CLIENT_DEFAULT
    ) -> QueryResult | None:
        query_str = builder.build()
        logger.debug("Prometheus Point Query: %s at time %s", query_str, time)

        params = {"query": query_str, "time": time.timestamp()}
        resp = await self.__httpx_client.get("/api/v1/query", params=params, timeout=request_timeout)

        if resp.status_code != 200:
            logger.critical("Error while querying Prometheus (status code: %d): %s", resp.status_code, resp.text)
            return None

        content = resp.json()
        if content.get("status") != "success":
            logger.critical("Prometheus query failed: %s", content)
            return None

        return parse_obj_as(QueryResult, content)

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
        request_timeout: float = httpx.USE_CLIENT_DEFAULT,
    ) -> QueryResult | None:
        query_str = builder.build()
        logger.debug("Prometheus Query Range: %s from %s to %s with step %s", query_str, start, end, step)

        resp = await self.__httpx_client.get("/api/v1/query_range", params={
            "query": query_str, "start": start.timestamp(), "end": end.timestamp(),
            "step": timedelta_to_prom_duration(step)
        }, timeout=request_timeout)

        if resp.status_code != 200:
            logger.critical("Error while querying Prometheus (status code: %d): %s", resp.status_code, resp.text)
            return None

        content = resp.json()
        if content.get("status") != "success":
            logger.critical("Prometheus query failed: %s", content)
            return None

        return parse_obj_as(QueryResult, content)


def timedelta_to_prom_duration(td: timedelta) -> str:
    total_ms = (
        td.days * 86_400_000
        + td.seconds * 1_000
        + td.microseconds // 1_000
    )

    units = [
        ("y",  365 * 24 * 60 * 60 * 1000),
        ("w",  7   * 24 * 60 * 60 * 1000),
        ("d",  24  * 60 * 60 * 1000),
        ("h",  60  * 60 * 1000),
        ("m",  60  * 1000),
        ("s",  1000),
        ("ms", 1),
    ]

    parts = []
    for suffix, ms_val in units:
        if total_ms >= ms_val:
            parts.append(f"{total_ms // ms_val}{suffix}")
            total_ms %= ms_val

    return "".join(parts) or '0s'