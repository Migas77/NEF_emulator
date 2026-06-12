import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from pymongo.synchronous.database import Database
from sqlalchemy.orm import Session

from app import schemas
from app.core.analyticsExposure import DEFAULT_POLL_INTERVAL
from app.core.analyticsExposure.handlers import handler_for_subscription
from app.core.analyticsExposure.utilities import send_notification, subscription_id_from_link
from app.crud import crud_mongo
from app.drivers.analyticsExposure import AnalyticsExposureDriver, get_analyticsExposure_driver
from app.schemas.analyticsExposure import AnalyticsEvent, AnalyticsEventNotif, AnalyticsEventNotification
from app.schemas.analyticsExposureInternal import AnalyticsExposureSubscWithState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
db_collection = "AnalyticsExposure"


@dataclass
class _PollerState:
    next_poll_time: float
    stop_loop: bool = False


class SubscriptionAnalyticsPoller:

    def __init__(self, driver: AnalyticsExposureDriver):
        self.poll_interval = DEFAULT_POLL_INTERVAL
        self.driver = driver

    async def poll(
        self,
        db_mongo: Database,
        db_sql: Session,
        *,
        subscription_id: str,
        db_event: asyncio.Event,
    ) -> None:
        try:
            await self._poll(db_mongo, db_sql, subscription_id=subscription_id, db_event=db_event)
        except Exception:
            logger.exception("Subscription %s: poller crashed", subscription_id)

    async def _poll(
        self,
        db_mongo: Database,
        db_sql: Session,
        *,
        subscription_id: str,
        db_event: asyncio.Event,
    ) -> None:
        subscription = self._load_subscription(db_mongo, subscription_id)
        if subscription is None:
            logger.error("Subscription %s not found", subscription_id)
            return

        logger.info("id=%s|notifId=%s: starting poller", subscription_id, subscription.notifId)
        send_notif, poll_interval = self._resolve_notification_strategy(subscription)
        loop = asyncio.get_running_loop()
        poll_state = _PollerState(next_poll_time=loop.time())

        while True:
            logger.info("id=%s|notifId=%s: polling", subscription_id, subscription.notifId)

            if db_event.is_set():
                subscription = self._load_subscription(db_mongo, subscription_id)
                if subscription is None:
                    logger.info("id=%s: subscription deleted, stopping poller", subscription_id)
                    return
                send_notif, poll_interval = self._resolve_notification_strategy(subscription)
                db_event.clear()

            if self._subscription_limits_reached(subscription):
                logger.info("id=%s|notifId=%s: limits reached, stopping poller", subscription_id, subscription.notifId)
                crud_mongo.update_new_field(db_mongo, db_collection, subscription_id, {"state.is_active": False})
                return

            notifs = await self._collect_event_notifs(db_sql, subscription)
            logger.info("id=%s|notifId=%s: collected %d notifs", subscription_id, subscription.notifId, len(notifs))
            if notifs:
                report = AnalyticsEventNotification(
                    notifId=subscription.notifId,
                    analyEventNotifs=notifs,
                )
                await send_notif(subscription, poll_state, report)
                subscription.state.report_count += 1
                crud_mongo.update_new_field(db_mongo, db_collection, subscription_id, {
                    "state.report_count": subscription.state.report_count
                })

            if poll_state.stop_loop:
                logger.info("id=%s|notifId=%s: stop condition fulfilled, stopping poller", subscription_id, subscription.notifId)
                crud_mongo.update_new_field(db_mongo, db_collection, subscription_id, {"state.is_active": False})
                return

            poll_state.next_poll_time += poll_interval
            sleep_for = poll_state.next_poll_time - loop.time()
            if sleep_for > 0.0:
                await asyncio.sleep(sleep_for)
            else:
                logger.warning("id=%s|notifId=%s: poll took longer than poll_interval, "
                               "skipping to next deadline", subscription_id, subscription.notifId)
                poll_state.next_poll_time = loop.time() + poll_interval

    async def _collect_event_notifs(
        self,
        db_sql: Session,
        subscription: schemas.AnalyticsExposureSubsc,
    ) -> list[AnalyticsEventNotif]:
        notifs = []
        for event_sub in subscription.analyEventsSubs:
            notif = await self._handle_event_sub(db_sql, subscription, event_sub)
            if notif:
                notifs.append(notif)
        return notifs

    def _subscription_limits_reached(
        self, subscription: AnalyticsExposureSubscWithState
    ) -> bool:
        rep_info = subscription.analyRepInfo
        sub_id = subscription_id_from_link(subscription.self)

        if rep_info.monDur is not None:
            mon_dur = rep_info.monDur if rep_info.monDur.tzinfo is not None else rep_info.monDur.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) >= mon_dur:
                logger.info("id=%s|notifId=%s: monitoring duration expired", sub_id, subscription.notifId)
                return True
        if rep_info.maxReportNbr is not None and 0 < rep_info.maxReportNbr <= subscription.state.report_count:
            logger.info("id=%s|notifId=%s: max report number (%d) reached", sub_id, subscription.notifId, rep_info.maxReportNbr)
            return True
        return False

    async def _handle_event_sub(
        self,
        db_sql: Session,
        subscription: schemas.AnalyticsExposureSubsc,
        event_sub: schemas.AnalyticsEventSubsc,
    ) -> AnalyticsEventNotif | None:
        sub_id = subscription_id_from_link(subscription.self)
        handler = handler_for_subscription(
            event_sub.analyEvent,
            db_sql=db_sql,
            driver=self.driver,
            subscription=subscription,
            analytics_event_subsc=event_sub,
        )
        if handler is None:
            logger.info("id=%s|notifId=%s: no handler for event %s, skipping", sub_id, subscription.notifId, event_sub.analyEvent)
            return None

        return await handler.get_analytics()

    def _load_subscription(
        self, db_mongo: Database, subscription_id: str
    ) -> AnalyticsExposureSubscWithState | None:
        doc = crud_mongo.read_uuid(db_mongo, db_collection, subscription_id)
        if doc is None:
            return None
        doc.pop("owner_id")
        return AnalyticsExposureSubscWithState(**doc)

    def _resolve_notification_strategy(
        self, subscription: schemas.AnalyticsExposureSubsc
    ) -> tuple[Callable, float]:
        method = subscription.analyRepInfo.notifMethod
        if method == schemas.NotificationMethod.periodic:
            period = subscription.analyRepInfo.repPeriod
            return self._send_periodic, float(period) if period else self.poll_interval
        if method == schemas.NotificationMethod.oneTime:
            return self._send_one_time, self.poll_interval
        # schemas.NotificationMethod.onEventDetection
        return self._send_on_event_detection, self.poll_interval

    async def _send_periodic(
        self, subscription: schemas.AnalyticsExposureSubsc, poll_state: _PollerState, payload: Any
    ) -> None:
        await send_notification(
            subscription.notifUri, payload,
            subscription_id=subscription_id_from_link(subscription.self),
            event=", ".join(str(n.analyEvent) for n in (payload.analyEventNotifs or [])),
        )

    async def _send_one_time(
        self, subscription: schemas.AnalyticsExposureSubsc, poll_state: _PollerState, payload: Any
    ) -> None:
        poll_state.stop_loop = await send_notification(
            subscription.notifUri, payload,
            subscription_id=subscription_id_from_link(subscription.self),
            event=", ".join(str(n.analyEvent) for n in (payload.analyEventNotifs or [])),
        )

    async def _send_on_event_detection(
        self, subscription: schemas.AnalyticsExposureSubsc, poll_state: _PollerState, payload: AnalyticsEventNotification
    ) -> None:
        filtered = [n for n in (payload.analyEventNotifs or []) if self._event_detected(n)]
        if filtered:
            await send_notification(
                subscription.notifUri,
                AnalyticsEventNotification(notifId=payload.notifId, analyEventNotifs=filtered),
                subscription_id=subscription_id_from_link(subscription.self),
                event=", ".join(str(n.analyEvent) for n in filtered),
            )

    @staticmethod
    def _event_detected(notif: AnalyticsEventNotif) -> bool:
        # The spec doesn't specify what exactly triggers the notification for non threshold events
        # Therefore, for the following events we will just send the notification if there is any data in the query result.
        if notif.analyEvent == AnalyticsEvent.wlanPerformance:
            return bool(notif.wlanInfos)
        if notif.analyEvent == AnalyticsEvent.ueComm:
            return bool(notif.ueCommInfos)
        # TODO: other event types use threshold-based notifications; not implemented (e.g. CONGESTION)
        return True


subscription_analytics_poller = SubscriptionAnalyticsPoller(get_analyticsExposure_driver())

