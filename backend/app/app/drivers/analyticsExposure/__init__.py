from typing import Type, Annotated
from dataclasses import dataclass
from fastapi import Depends
from app.core.config import settings, AnalyticsExposureBackend
from ...interfaces.analyticsExposure.Query import Query, QueryArgs, QueryCatalog
from ...interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface
from ...interfaces.analyticsExposure.QueryServiceInterface import QueryServiceInterface


@dataclass
class AnalyticsExposureDriver:
    QueryArgsCls: Type[QueryArgs]
    QueryCls: Type[Query]
    query_catalog: QueryCatalog
    query_builder: QueryBuilderInterface
    query_service: QueryServiceInterface


from .prom.Query import PromQuery, PromQueryArgs, PromQueryCatalog
from .prom.QueryBuilder import PromQueryBuilder

if settings.analyticsExposure.backend == AnalyticsExposureBackend.PROMETHEUS:
    from .prom.QueryService import PromQueryService
    _query_service = PromQueryService(settings.analyticsExposure.url)
else:
    from app.db.session import SessionLocal
    from .stub.QueryService import StubQueryService
    _query_service = StubQueryService(SessionLocal)

_driver = AnalyticsExposureDriver(
    QueryArgsCls=PromQueryArgs,
    QueryCls=PromQuery,
    query_catalog=PromQueryCatalog(),
    query_builder=PromQueryBuilder(),
    query_service=_query_service,
)


def get_analyticsExposure_driver() -> AnalyticsExposureDriver:
    return _driver


AnalyticsExposureDep = Annotated[AnalyticsExposureDriver, Depends(get_analyticsExposure_driver)]

