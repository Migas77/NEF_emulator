import asyncio
import logging

from fastapi import APIRouter
from pydantic import ValidationError
from starlette.responses import Response

from app.core.notification_responder import notification_responder
from app.db.session import client
from app.crud import crud_mongo
from app.schemas import AnalyticsEventNotification, AnalyticsExposureSubsc
from app.schemas.analyticsAlertIngestion import AlertNotification, Alert
from app.schemas.analyticsExposure import AnalyticsEvent

router = APIRouter()
db_collection = "AnalyticsExposure"

@router.post(
    "",
    responses={204: {"model": None}}
)
async def ingest_alert(alert_notification: AlertNotification):
    db_mongo = client.fastapi

    for alert in alert_notification.alerts:
        subscriptionId = alert.labels.get("subscriptionId")
        if not subscriptionId:
            logging.critical("Alert missing subscriptionId label: %s", alert)
            continue
        retrieved_doc = crud_mongo.read_uuid(db_mongo, db_collection, subscriptionId)
        if not retrieved_doc:
            logging.critical("No document found for subscriptionId %s, skipping alert: %s", subscriptionId, alert)
            continue

        retrieved_doc.pop("owner_id")

        try:
            analytics_subs = AnalyticsExposureSubsc.parse_obj(retrieved_doc)
        except ValidationError as e:
            logging.critical("Failed to validate AnalyticsExposureSubsc for subscriptionId %s: %s", subscriptionId, e.errors())
            continue

        asyncio.create_task(send_analytics_notification_callback(analytics_subs, alert))

    return Response(status_code=204)


async def send_analytics_notification_callback(analytics_sub: AnalyticsExposureSubsc, alert: Alert):

    #TODO resolved events will also come here

    logging.info(
        "Attempting to send the callback to %s",
        analytics_sub.notifUri
    )

    subscriptionId = alert.labels.get("subscriptionId")
    analy_event_notifs = []
    for analy_event_sub in analytics_sub.analyEventsSubs:

        if analy_event_sub.analyEvent not in [AnalyticsEvent.dnPerformance, AnalyticsEvent.e2eDataVolTransTime]:
            # not supported
            logging.warning("Unsupported analyEvent %s, skipping for subscriptionId %s", analy_event_sub.analyEvent, subscriptionId)
            continue


    notification = AnalyticsEventNotification(
        notifId=1,
        analyEventNotifs=analy_event_notifs
    )

    # await notification_responder.send_notification(analytics_sub.notifUri, notification)