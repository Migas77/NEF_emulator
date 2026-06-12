from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, TYPE_CHECKING

from typing_extensions import override

from app.schemas.analyticsExposure import AnalyticsData, AnalyticsEventNotif
from app.schemas.analyticsExposureInternal import QueryResult

if TYPE_CHECKING:
    from app.core.analyticsExposure.handlers.base import AnalyticsHandler

T = TypeVar('T')

class BaseDispatcher(ABC, Generic[T]):

    @abstractmethod
    def results_to_analytics_payload(self, handler: AnalyticsHandler[T], results: list[QueryResult | None]) -> T | None:
        # Converts raw query results into the appropriate analytics payload
        # AnalyticsEventNotif for subscription, AnalyticsData for fetch
        pass


class FetchDispatcher(BaseDispatcher[AnalyticsData]):

    @override
    def results_to_analytics_payload(
        self, handler: AnalyticsHandler[AnalyticsData], results: list[QueryResult | None]
    ) -> AnalyticsData | None:
        return handler.results_to_analytics_data(results)


class SubscriptionDispatcher(BaseDispatcher[AnalyticsEventNotif]):

    @override
    def results_to_analytics_payload(
        self, handler: AnalyticsHandler[AnalyticsEventNotif], results: list[QueryResult | None]
    ) -> AnalyticsEventNotif | None:
        return handler.results_to_analytics_event_notif(results)

