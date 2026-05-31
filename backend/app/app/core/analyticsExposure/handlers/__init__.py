import logging

from sqlalchemy.orm import Session

from app.core.analyticsExposure.handlers.base import AnalyticsHandler
from app.core.analyticsExposure.handlers.ue_comm import UeCommHandler
from app.core.analyticsExposure.handlers.wlan_performance import WlanPerformanceHandler
from app.drivers.analyticsExposure import AnalyticsExposureDriver
from app.schemas.analyticsExposure import AnalyticsEvent, AnalyticsExposureSubsc, AnalyticsEventSubsc, AnalyticsRequest


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


_registry: dict[AnalyticsEvent, type[AnalyticsHandler]] = {
    AnalyticsEvent.ueComm: UeCommHandler,
    AnalyticsEvent.wlanPerformance: WlanPerformanceHandler,
}


def handler_for_subscription(
    event: AnalyticsEvent,
    *,
    db_sql: Session,
    driver: AnalyticsExposureDriver,
    subscription: AnalyticsExposureSubsc,
    analytics_event_subsc: AnalyticsEventSubsc
) -> AnalyticsHandler | None:
    cls = _registry.get(event)
    if cls is None:
        logger.warning("No handler found for event %s", event)
        return None
    return cls.create_for_subscription(db_sql, driver, subscription, analytics_event_subsc)


def handler_for_fetch(
    event: AnalyticsEvent,
    *,
    db_sql: Session,
    driver: AnalyticsExposureDriver,
    analytics_request: AnalyticsRequest
) -> AnalyticsHandler | None:
    cls = _registry.get(event)
    if cls is None:
        logger.warning("No handler found for event %s", event)
        return None
    return cls.create_for_fetch(db_sql, driver, analytics_request)

