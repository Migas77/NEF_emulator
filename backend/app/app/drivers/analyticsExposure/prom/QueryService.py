import logging
import httpx
from datetime import datetime, timedelta
from app.interfaces.analyticsExposure.QueryServiceInterface import QueryServiceInterface
from app.schemas.analyticsExposureInternal import VectorQueryResult, MatrixQueryResult, ScalarQueryResult, \
    StringQueryResult, QueryResult

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _parse_result(content: dict) -> QueryResult | None:
    data = content["data"]
    result_type = data["resultType"]
    payload = {"status": content["status"], "result_type": result_type, "result": data["result"]}
    if result_type == "vector":
        return VectorQueryResult(**payload)
    if result_type == "matrix":
        return MatrixQueryResult(**payload)
    if result_type == "scalar":
        return ScalarQueryResult(**payload)
    if result_type == "string":
        return StringQueryResult(**payload)
    logger.warning("Unsupported Prometheus resultType: %s", result_type)
    return None


class PromQueryService(QueryServiceInterface):

    def __init__(self, prometheus_url):
        self.__httpx_client = httpx.AsyncClient(
            base_url=prometheus_url,
            timeout=httpx.Timeout(20, connect=3.05, read=27)
        )

    async def query(self,
        query: str,
        *,
        time: datetime | None = None,
        timeout: timedelta | None = None,
        limit: int | None = None,
        lookback_delta: float | None = None,
        stats: str | None = None,
        request_timeout: float = httpx.USE_CLIENT_DEFAULT
    ) -> QueryResult | None:
        params = {"query": query}
        params |= {"time": time.timestamp()} if time else {}
        logger.info("Prometheus Point Query: %s at time %s", query, time)
        resp = await self.__httpx_client.get("/api/v1/query", params=params, timeout=request_timeout)

        if resp.status_code != 200:
            logger.critical("Error while querying Prometheus (status code: %d): %s", resp.status_code, resp.text)
            return None

        return _parse_result(resp.json())

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
        request_timeout: float = httpx.USE_CLIENT_DEFAULT,
    ) -> QueryResult | None:
        logger.info("Prometheus Query Range: %s from %s to %s with step %s", query, start, end, step)
        resp = await self.__httpx_client.get("/api/v1/query_range", params={
            "query": query, "start": start.timestamp(), "end": end.timestamp(),
            "step": timedelta_to_prom_duration(step)
        }, timeout=request_timeout)

        if resp.status_code != 200:
            logger.critical("Error while querying Prometheus (status code: %d): %s", resp.status_code, resp.text)
            return None

        return _parse_result(resp.json())


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