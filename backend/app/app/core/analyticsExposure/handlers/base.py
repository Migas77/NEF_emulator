import logging
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
from app.interfaces.analyticsExposure.Query import Query
from app.interfaces.analyticsExposure.QueryBuilderInterface import QueryBuilderInterface
from app.schemas import NotificationMethod
from app.schemas.analyticsExposure import (
    AnalyticsData,
    AnalyticsEventSubsc,
    AnalyticsExposureSubsc,
    AnalyticsEventNotif,
    EventReportingRequirement, AnalyticsRequest
)
from app.schemas.analyticsExposureInternal import QueryResult

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
    queries: list[Query]
    _built_queries: list[tuple[QueryBuilderInterface, QueryType]]

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
        self.queries = []
        self._built_queries = []

    @classmethod
    def create_for_subscription(
        cls,
        db_sql: Session,
        driver: AnalyticsExposureDriver,
        subscription: AnalyticsExposureSubsc,
        analytics_event_subsc: AnalyticsEventSubsc,
    ) -> "AnalyticsHandler[AnalyticsEventNotif] | None":
        analytics_id = f"op=sub|id={subscription_id_from_link(subscription.self)}|notifId={subscription.notifId}"
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
        analytics_id = f"op=fetch|id={uuid4()}"
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

    async def _execute_queries(self) -> list[QueryResult | None]:
        results = []
        for builder, query_type in self._built_queries:
            # logger.info("%s: executing query type=%s", self.analytics_id, query_type)
            if query_type == QueryType.QUERY:
                sub_result = await self.driver.query_service.query(builder, time=self.query_end_ts)
            else:
                sub_result = await self.driver.query_service.query_range(
                    builder, start=self.query_start_ts, end=self.query_end_ts, step=self.query_step
                )
            results.append(sub_result)

        return results

    async def get_analytics(self) -> T | None:
        if not self.target_ues:
            logger.info("%s: no target UEs found, skipping", self.analytics_id)
            return None

        self._set_event_queries()
        if not self.queries:
            logger.info("%s: no queries set for event, skipping", self.analytics_id)
            return []

        self.set_built_queries()
        if not self._built_queries:
            logger.info("%s: no queries to execute, skipping", self.analytics_id)
            return None

        results = await self._execute_queries()
        return self.dispatcher.results_to_analytics_payload(self, results)

    def _setup_for_subscription(self) -> "AnalyticsHandler[AnalyticsEventNotif] | None":
        """Override to perform extra subscription query setup. Return None to abort handler creation."""
        return self

    def _setup_for_fetch(self) -> "AnalyticsHandler[AnalyticsData] | None":
        """Override to perform extra fetch query setup. Return None to abort handler creation."""
        return self

    @abstractmethod
    def _set_event_queries(self) -> None:
        """Populate self.queries with the Query catalog entries the event requires."""
        pass

    @abstractmethod
    def set_built_queries(self) -> None:
        """Pair each query in self.queries with its args and using QueryBuilderInterface
        assign the resulting (builder, query_type) pairs to self._built_queries."""
        pass

    @abstractmethod
    def results_to_analytics_event_notif(self, results: list) -> AnalyticsEventNotif | None:
        """For subscription analytics, convert query results into an AnalyticsEventNotif to be sent as a notification."""
        pass

    @abstractmethod
    def results_to_analytics_data(self, results: list) -> AnalyticsData | None:
        """For fetch analytics, convert query results into an AnalyticsData to be returned in the fetch response."""
        pass

