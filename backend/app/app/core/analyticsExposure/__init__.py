from datetime import datetime, timedelta, timezone

from app.core.config import settings
from app.schemas.analyticsExposure import EventReportingRequirement

DEFAULT_POLL_INTERVAL: float = settings.analyticsExposure.poll_interval
DEFAULT_TEMPORAL_GRAN_SIZE: int = settings.analyticsExposure.temporal_gran_size


def default_event_reporting_requirement() -> EventReportingRequirement:
    start_ts = datetime.now(timezone.utc) + timedelta(seconds=settings.analyticsExposure.offset_period)
    return EventReportingRequirement(startTs=start_ts)

