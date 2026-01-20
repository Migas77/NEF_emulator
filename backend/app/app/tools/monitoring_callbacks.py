import logging
import asyncio
from typing import Optional
from collections.abc import Generator

from app import crud
from app.core.notification_responder import notification_responder
from app.models.UE import UE
from app.db.session import client
from app.schemas.commonData import PlmnId
from app.schemas.monitoringevent import (
    GeographicalCoordinates,
    LocationInfo,
    LocationType,
    MonitoringEventReport,
    MonitoringNotification,
    MonitoringType,
    ReachabilityType,
    Point,
    SupportedGADShapes,
)

from app.api.deps import db_context


def get_subscription_mon_types(sub) -> Generator[MonitoringType]:
    yield sub["monitoringType"]

    if sub.get("addnMonTypes") is not None:
        for monType in sub.get("addnMonTypes"):
            yield monType


def update_maximum_reports(sub, id):
    db_mongo = client.fastapi
    if sub.get("maximumNumberOfReports") is not None:
        db_mongo["MonitoringEvent"].find_one_and_update(
            {"_id": id},
            {"$inc": {"subscription.maximumNumberOfReports": -1}},
        )


async def handle_location_report_callback(location_reporting_sub, ue: UE, doc_id):
    logging.info(
        "Attempting to send the callback to %d",
        location_reporting_sub.get("notificationDestination"),
    )

    notification = MonitoringNotification(
        subscription=location_reporting_sub.get("self"),
        monitoringEventReports=[create_location_event_report(ue, location_reporting_sub.get("locationType"))],
    )

    try:
        await notification_responder.send_notification(
            location_reporting_sub.get("notificationDestination"), notification
        )
        update_maximum_reports(location_reporting_sub, doc_id)
    except Exception as ex:
        raise ex


def create_location_event_report(ue: UE, locationType: Optional[LocationType] = LocationType.CURRENT_LOCATION) -> MonitoringEventReport:
    if locationType == LocationType.CURRENT_LOCATION:
        cellid = ue.Cell_id if ue.Cell_id is not None else None
    elif locationType == LocationType.LAST_KNOWN_LOCATION:
        cellid = ue.last_known_cell_id
    elif locationType == LocationType.INITIAL_LOCATION:
        cellid = ue.initial_cell_id
    elif locationType == LocationType.CURRENT_OR_LAST_KNOWN_LOCATION:
        cellid = ue.Cell_id if ue.Cell_id is not None else ue.last_known_cell_id

    cell = None
    with db_context() as db:
        cell = crud.cell.get_Cell_id_by_id(db=db, id=cellid)
    report = MonitoringEventReport(
        externalId=ue.external_identifier,
        monitoringType=MonitoringType.LOCATION_REPORTING,
        locationInfo=LocationInfo(
            cellId=cell.cell_id if cell is not None else None,
            geographicArea=Point(
                shape=SupportedGADShapes.POINT,
                point=GeographicalCoordinates(
                    lat=ue.latitude,
                    lon=ue.longitude,
                ),
            ),
        ),
    )

    if ue.Cell_id is not None and report.locationInfo is not None:
        report.locationInfo.cellId = ue.Cell.cell_id

    return report


async def handle_loss_connectivity_callback(
    loss_of_connectivity_sub,
    ue: UE,
    doc_id,
    old_cell_id: Optional[str],
    current_cell_id: Optional[str],
):
    if old_cell_id is None or current_cell_id is not None:
        return

    lossOfConnectReason = 6  #  6 = UE is deregistered

    if loss_of_connectivity_sub.get("maximumDetectionTime") is not None:
        await asyncio.sleep(loss_of_connectivity_sub.get("maximumDetectionTime"))
        with db_context() as db:
            new_ue = crud.ue.get_supi(db=db, supi=ue.supi)
            if new_ue is None or new_ue.Cell_id is not None:
                return

        lossOfConnectReason = 7  # 7 = Maximum detection timer expires

    await send_loss_connectivity_callback(
        loss_of_connectivity_sub, ue, doc_id, lossOfConnectReason
    )


async def send_loss_connectivity_callback(
    loss_of_connectivity_sub, ue: UE, doc_id, lossOfConnectReason: int
):
    notification = MonitoringNotification(
        subscription=loss_of_connectivity_sub.get("self"),
        monitoringEventReports=[
            create_loss_of_connectivity_event_report(ue, lossOfConnectReason)
        ],
    )

    try:
        await notification_responder.send_notification(
            loss_of_connectivity_sub.get("notificationDestination"),
            notification,
        )

        update_maximum_reports(loss_of_connectivity_sub, doc_id)
    except Exception as ex:
        raise ex


def create_loss_of_connectivity_event_report(
    ue: UE, lossOfConnectReason: int
) -> MonitoringEventReport:
    return MonitoringEventReport(
        externalId=ue.external_identifier,
        monitoringType=MonitoringType.LOSS_OF_CONNECTIVITY,
        lossOfConnectReason=lossOfConnectReason,
    )


async def handle_ue_reachability_callback(
    subscription,
    ue: UE,
    doc_id,
    old_cell_id: Optional[str],
    current_cell_id: Optional[str],
):
    if current_cell_id is None or old_cell_id is not None:
        return

    return await send_ue_reachability_callback(subscription, ue, doc_id)


async def send_ue_reachability_callback(
    subscription,
    ue: UE,
    doc_id,
):
    notification = MonitoringNotification(
        subscription=subscription.get("self"),
        monitoringEventReports=[
            create_ue_reachability_event_report(
                ue, subscription.get("reachabilityType")
            )
        ],
    )

    try:
        await notification_responder.send_notification(
            subscription.get("notificationDestination"), notification
        )
        update_maximum_reports(subscription, doc_id)
    except Exception as ex:
        raise ex


def create_ue_reachability_event_report(
    ue: UE, reachability_type: ReachabilityType
) -> MonitoringEventReport:
    return MonitoringEventReport(
        externalId=ue.external_identifier,
        monitoringType=MonitoringType.UE_REACHABILITY,
        reachabilityType=reachability_type,
    )


async def send_roaming_status_callback(
    subscription,
    ue: UE,
    doc_id,
):
    notification = MonitoringNotification(
        subscription=subscription.get("self"),
        monitoringEventReports=[
            create_roaming_status_event_report(ue, subscription.get("plmnIndication"))
        ],
    )

    try:
        await notification_responder.send_notification(
            subscription.get("notificationDestination"), notification
        )
        update_maximum_reports(subscription, doc_id)
    except Exception as ex:
        raise ex


def create_roaming_status_event_report(
    ue: UE, plmnIndication: Optional[bool]
) -> MonitoringEventReport:
    report = MonitoringEventReport(
        externalId=ue.external_identifier,
        monitoringType=MonitoringType.ROAMING_STATUS,
        roamingStatus=ue.visiting_plmnid is not None,
    )

    if plmnIndication:
        if ue.visiting_plmnid is not None:
            parts = ue.visiting_plmnid.split("-")
            mcc = int(parts[0], 10)
            mnc = int(parts[1], 10)
            report.plmnId = PlmnId(mcc=mcc, mnc=mnc)
        else:
            report.plmnId = PlmnId(mcc=ue.mcc, mnc=ue.mnc)

    return report
