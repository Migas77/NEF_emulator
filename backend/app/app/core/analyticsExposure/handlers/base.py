import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta, datetime
from enum import Enum, auto
from typing import Callable, TypeVar, Generic
from uuid import uuid4

from sqlalchemy.orm import Session

from app import schemas
from app.core.analyticsExposure import DEFAULT_TEMPORAL_GRAN_SIZE, default_event_reporting_requirement
from app.core.analyticsExposure.handlers.dispatcher import BaseDispatcher, SubscriptionDispatcher, \
    FetchDispatcher
from app.core.analyticsExposure.utilities import resolve_query_start_end_ts, get_subsc_ues, subscription_id_from_link
from app.drivers.analyticsExposure import AnalyticsExposureDriver
from app.interfaces.analyticsExposure.Query import QueryCatalog, Query
from app.schemas import NotificationMethod
from app.schemas.analyticsExposure import (
    AnalyticsData,
    AnalyticsEventSubsc,
    AnalyticsExposureSubsc,
    AnalyticsEventNotif,
    EventReportingRequirement, AnalyticsRequest
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class FieldMapping:
    field: str
    extractor: str
    converter: Callable


class QueryType(Enum):
    QUERY = auto()                              # query at a specific moment in time
    QUERY_RANGE = auto()                        # query over a time range with start, end and step


class AnalyticsHandler(ABC, Generic[T]):

    # Common
    db_sql: Session
    driver: AnalyticsExposureDriver
    analytics_id: str
    dispatcher: BaseDispatcher[T]
    target_ues: list[schemas.UE]
    reporting_requirement: EventReportingRequirement
    query_start_ts: datetime
    query_end_ts: datetime
    query_step: timedelta
    query_interval: timedelta
    query_type: QueryType
    temporal_gran_size: timedelta

    # Subscription Path
    subscription: AnalyticsExposureSubsc | None = None
    analytics_event_subsc: AnalyticsEventSubsc | None = None

    # Fetch Path
    analytics_request: AnalyticsRequest | None = None

    def __init__(
        self,
        db_sql: Session,
        driver: AnalyticsExposureDriver,
        analytics_id: str,
        dispatcher: BaseDispatcher,
        a: AnalyticsEventSubsc | AnalyticsRequest,
        query_start_ts: datetime,
        query_end_ts: datetime,
        reporting_requirement: EventReportingRequirement | None,
        temporal_gran_size: float | None,
        *,
        subscription: AnalyticsExposureSubsc | None = None,
    ):
        self.db_sql = db_sql
        self.driver = driver
        self.analytics_id = analytics_id
        self.dispatcher = dispatcher
        self.subscription = subscription
        self.analytics_event_subsc, self.analytics_request = (a, None) if isinstance(a, AnalyticsEventSubsc) else (None, a)
        self.target_ues = get_subsc_ues(db_sql, a)
        self.reporting_requirement = reporting_requirement
        self.query_start_ts = query_start_ts
        self.query_end_ts = query_end_ts
        self.temporal_gran_size = timedelta(seconds=temporal_gran_size)
        self.query_step = timedelta(seconds=temporal_gran_size)
        self.query_interval = timedelta(seconds=temporal_gran_size)
        self.query_type = QueryType.QUERY if query_start_ts == query_end_ts else QueryType.QUERY_RANGE

    @classmethod
    def create_for_subscription(
        cls,
        db_sql: Session,
        driver: AnalyticsExposureDriver,
        subscription: AnalyticsExposureSubsc,
        analytics_event_subsc: AnalyticsEventSubsc,
    ) -> "AnalyticsHandler[AnalyticsEventNotif] | None":
        analytics_id = f"sub-{subscription_id_from_link(subscription.self)}"
        analytics_filter = analytics_event_subsc.analyEventFilter
        extra_report_req, temporal_gran_size = (
            analytics_filter.extraReportReq or default_event_reporting_requirement(),
            analytics_filter.temporalGranSize or DEFAULT_TEMPORAL_GRAN_SIZE,
        ) if analytics_filter else (default_event_reporting_requirement(), DEFAULT_TEMPORAL_GRAN_SIZE)

        rep_period = (
            timedelta(seconds=subscription.analyRepInfo.repPeriod)
            if subscription.analyRepInfo.notifMethod == NotificationMethod.periodic and
               subscription.analyRepInfo.repPeriod is not None
            else None
        )

        offset_period = (
            timedelta(seconds=extra_report_req.offsetPeriod)
            if extra_report_req.offsetPeriod is not None
            else None
        )

        timestamps = resolve_query_start_end_ts(
            offset_period=offset_period,
            rep_period=rep_period,
            start_ts=extra_report_req.startTs,
            end_ts=extra_report_req.endTs,
        )
        if timestamps is None:
            logger.warning("Could not resolve query timestamps for subscription %s, skipping", analytics_id)
            return None

        query_start_ts, query_end_ts = timestamps
        handler = cls(
            db_sql=db_sql, driver=driver, analytics_id=analytics_id,
            dispatcher=SubscriptionDispatcher(), a=analytics_event_subsc,
            query_start_ts=query_start_ts, query_end_ts=query_end_ts,
            reporting_requirement=extra_report_req,
            temporal_gran_size=temporal_gran_size,
            subscription=subscription,
        )

        return handler._setup_for_subscription()

    @classmethod
    def create_for_fetch(
        cls,
        db_sql: Session,
        driver: AnalyticsExposureDriver,
        analytics_request: AnalyticsRequest
    ) -> "AnalyticsHandler[AnalyticsData] | None":
        analytics_id = f"fetch-{uuid4()}"
        analy_rep = analytics_request.analyRep or default_event_reporting_requirement()
        temporal_gran_size = (
            analytics_request.analyEventFilter.temporalGranSize or DEFAULT_TEMPORAL_GRAN_SIZE
            if analytics_request.analyEventFilter else DEFAULT_TEMPORAL_GRAN_SIZE
        )

        timestamps = resolve_query_start_end_ts(start_ts=analy_rep.startTs, end_ts=analy_rep.endTs)
        if timestamps is None:
            logger.warning("%s: Could not resolve query timestamps for fetch request, skipping", analytics_id)
            return None

        query_start_ts, query_end_ts = timestamps
        handler = cls(
            db_sql=db_sql, driver=driver, analytics_id=analytics_id,
            dispatcher=FetchDispatcher(), a=analytics_request,
            query_start_ts=query_start_ts, query_end_ts=query_end_ts,
            reporting_requirement=analy_rep,
            temporal_gran_size=temporal_gran_size
        )

        return handler._setup_for_fetch()

    async def _execute_queries(self, prepared_queries: list[tuple[str, QueryType]]) -> list:
        results = []
        for built_query, query_type in prepared_queries:
            # logger.info("%s: executing query type=%s", self.analytics_id, query_type)
            if query_type == QueryType.QUERY:
                sub_result = await self.driver.query_service.query(built_query, time=self.query_start_ts)
            else:
                sub_result = await self.driver.query_service.query_range(
                    built_query, start=self.query_start_ts + self.query_step, end=self.query_end_ts, step=self.query_step
                )
            if sub_result and sub_result.is_success and isinstance(sub_result.result, list):
                results.extend(sub_result.result)
            else:
                logger.info("%s: query failed or returned no results", self.analytics_id)

        if not results:
            logger.info("%s: all queries returned empty", self.analytics_id)

        return results

    async def get_analytics(self) -> T | None:
        if not self.target_ues:
            logger.info("%s: no target UEs found, skipping", self.analytics_id)
            return None

        prepared_queries = self._prepare_queries()
        if not prepared_queries:
            logger.info("%s: no queries to execute, skipping", self.analytics_id)
            return None

        results = await self._execute_queries(prepared_queries)
        return self.dispatcher.results_to_analytics_payload(self, results)

    def _setup_for_subscription(self) -> "AnalyticsHandler[AnalyticsEventNotif] | None":
        """Override to perform extra subscription query setup. Return None to abort handler creation."""
        return self

    def _setup_for_fetch(self) -> "AnalyticsHandler[AnalyticsData] | None":
        """Override to perform extra fetch query setup. Return None to abort handler creation."""
        return self

    @abstractmethod
    def _get_event_metrics(self, catalog: QueryCatalog) -> list[Query]:
        pass

    @abstractmethod
    def _prepare_queries(self) -> list[tuple[str, QueryType]]:
        """Returns a list of tuples (query_str, query_type)"""
        pass

    @abstractmethod
    def results_to_analytics_event_notif(self, results: list) -> AnalyticsEventNotif | None:
        """For subscription analytics, convert query results into an AnalyticsEventNotif to be sent as a notification."""
        pass

    @abstractmethod
    def results_to_analytics_data(self, results: list) -> AnalyticsData | None:
        """For fetch analytics, convert query results into an AnalyticsData to be returned in the fetch response."""
        pass

