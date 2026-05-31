import asyncio
import logging
from typing import Any, List

from bson.objectid import ObjectId

from app.api.deps import get_db
from app.core.analyticsExposure.handlers import handler_for_fetch
from app.core.analyticsExposure.subscription_analytics_poller import subscription_analytics_poller
from app.core.analyticsExposure.utilities import get_subsc_ues
from app.core.analyticsExposure.subscription_task_registry import subscription_task_registry
from app.drivers.analyticsExposure import AnalyticsExposureDriver, AnalyticsExposureDep
from app.schemas.analyticsExposure import (
    AnalyticsEvent,
    AnalyticsEventFilterSubsc,
    AnalyticsEventSubsc,
    AnalyticsExposureSubsc,
    AnalyticsData,
)
from app.schemas.analyticsExposureInternal import AnalyticsState
from app.schemas.commonData import WebsockNotifConfig
from app.schemas.monitoringevent import GeographicalCoordinates, Point
from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from sqlalchemy.orm import Session
from app import models, schemas
from app.crud import crud_mongo, user, ue
from app.api import deps
from app.db.session import client
from app.api.api_v1.endpoints.utils import add_notifications, ReportLogging

router = APIRouter()
router.route_class = ReportLogging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_collection = "AnalyticsExposure"
supported_subscription_events = [AnalyticsEvent.ueComm, AnalyticsEvent.wlanPerformance]

@router.on_event("startup")
async def populate_task_registry():
    db_mongo = client.fastapi
    existing_subscriptions = crud_mongo.read_all_by_multiple_pairs(
        db_mongo, db_collection,
        **{"analyEventsSubs.analyEvent": {"$in": supported_subscription_events}, "state.is_active": True}
    )
    for subscription in existing_subscriptions:
        subscription_id_str = str(subscription["_id"])
        db_sql = next(get_db())
        db_event = asyncio.Event()
        subscription_task_registry.register(
            subscription_id_str,
            task=asyncio.create_task(subscription_analytics_poller.poll(
                db_mongo=db_mongo,
                db_sql=db_sql,
                subscription_id=subscription_id_str,
                db_event=db_event
            )),
            event=db_event
        )



@router.get(
    "/{afId}/subscriptions",
    response_model=List[schemas.AnalyticsExposureSubsc],
    responses={204: {"model": None}},
)
def read_active_subscriptions(
    *,
    afId: str = Path(
        ...,
        title="The ID of the Netapp that read all the subscriptions",
        example="myNetapp",
    ),
    current_user: models.User = Depends(deps.get_current_active_user),
    http_request: Request,
) -> Any:
    """
    Read all active subscriptions
    """
    db_mongo = client.fastapi
    retrieved_docs = crud_mongo.read_all(db_mongo, db_collection, current_user.id)

    if not retrieved_docs:
        raise HTTPException(status_code=404, detail="There are no active subscriptions")

    logger.info("Read %d active subscriptions for user %s", len(retrieved_docs), current_user.id)
    http_response = JSONResponse(content=retrieved_docs, status_code=200)
    add_notifications(http_request, http_response, False)
    return http_response


# #Callback

# analytics_exposure_callback_router = APIRouter()

# @analytics_exposure_callback_router.post("{$request.body.notificationDestination}", response_model=schemas.MonitoringEventReportReceived, status_code=200, response_class=Response)
# def analytics_exposure_notification(body: schemas.AnalyticsEventNotification):
#     pass


@router.post(
    "/{afId}/subscriptions",
    response_model=schemas.AnalyticsExposureSubsc,
    responses={201: {"model": schemas.AnalyticsExposureSubsc}},
)
async def create_subscription(
    *,
    afId: str = Path(
        ...,
        title="The ID of the Netapp that creates a subscription",
        example="myNetapp",
    ),
    db: Session = Depends(deps.get_db),
    item_in: schemas.AnalyticsExposureSubscUpsert,
    current_user: models.User = Depends(deps.get_current_active_user),
    http_request: Request,
) -> Any:
    """
    Create new subscription.
    """

    subscriptionId = ObjectId()
    for event_sub in item_in.analyEventsSubs:

        if event_sub.analyEvent not in supported_subscription_events:
            raise HTTPException(
                status_code=501, detail=f"Analytics Event {event_sub.analyEvent} has not been implemented"
            )

        if not (
            event_sub.tgtUe
            and any(
                [event_sub.tgtUe.gpsi, event_sub.tgtUe.anyUeInd, event_sub.tgtUe.exterGroupId]
            )
        ):
            raise HTTPException(status_code=404, detail="No Target UE Specified")

        user_equipments = get_subsc_ues(db, event_sub, owner_id=current_user.id)
        if not user_equipments:
            raise HTTPException(status_code=404, detail=f"Device not found for tgtUe {event_sub.tgtUe}")

    db_mongo = client.fastapi
    json_data = jsonable_encoder(item_in)
    json_data.update({"owner_id": current_user.id, "_id": subscriptionId, "state": jsonable_encoder(AnalyticsState())})

    inserted_doc = crud_mongo.create(db_mongo, db_collection, json_data)

    # Create the reference resource and location header
    link = str(http_request.url) + "/" + str(inserted_doc.inserted_id)
    response_header = {"location": link}

    # Update the subscription with the new resource (link) and return the response (+response header)
    crud_mongo.update_new_field(
        db_mongo, db_collection, inserted_doc.inserted_id, {"self": link}
    )

    # Retrieve the updated document | UpdateResult is not a dict
    updated_doc = crud_mongo.read_uuid(
        db_mongo, db_collection, inserted_doc.inserted_id
    )

    updated_doc.pop("owner_id")  # Remove owner_id from the response
    updated_doc.pop("state")     # Remove state from the response

    db_event = asyncio.Event()
    subscription_id_str = str(subscriptionId)
    subscription_task_registry.register(
        subscription_id_str,
        task=asyncio.create_task(subscription_analytics_poller.poll(
            db_mongo=db_mongo,
            db_sql=db,
            subscription_id=subscription_id_str,
            db_event=db_event
        )),
        event=db_event
    )

    http_response = JSONResponse(
        content=updated_doc, status_code=201, headers=response_header
    )
    add_notifications(http_request, http_response, False)

    return http_response


@router.put(
    "/{afId}/subscriptions/{subscriptionId}",
    response_model=schemas.AnalyticsExposureSubsc,
)
def update_subscription(
    *,
    afId: str = Path(
        ...,
        title="The ID of the Netapp that creates a subscription",
        example="myNetapp",
    ),
    subscriptionId: str = Path(..., title="Identifier of the subscription resource"),
    db: Session = Depends(deps.get_db),
    item_in: schemas.AnalyticsExposureSubscUpsert,
    current_user: models.User = Depends(deps.get_current_active_user),
    http_request: Request,
) -> Any:
    """
    Update/Replace an existing subscription resource by id
    """
    db_mongo = client.fastapi

    try:
        retrieved_doc = crud_mongo.read_uuid(db_mongo, db_collection, subscriptionId)
    except Exception as ex:
        raise HTTPException(
            status_code=400,
            detail="Please enter a valid uuid (24-character hex string)",
        )

    # Check if the document exists
    if not retrieved_doc:
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    # If the document exists then validate the owner
    if not user.is_superuser(current_user) and (
        retrieved_doc["owner_id"] != current_user.id
    ):
        raise HTTPException(status_code=400, detail="Not enough permissions")

    for event_sub in item_in.analyEventsSubs:

        if event_sub.analyEvent not in supported_subscription_events:
            raise HTTPException(
                status_code=501, detail=f"Analytics Event {event_sub.analyEvent} has not been implemented"
            )

        if not (
            event_sub.tgtUe
            and any(
                [event_sub.tgtUe.gpsi, event_sub.tgtUe.anyUeInd, event_sub.tgtUe.exterGroupId]
            )
        ):
            raise HTTPException(status_code=404, detail="No Target UE Specified")

        user_equipments = get_subsc_ues(db, event_sub, owner_id=current_user.id)
        if not user_equipments:
            raise HTTPException(status_code=404, detail=f"Device not found for tgtUe {event_sub.tgtUe}")

    # Set resource reference (ignore suggested self)
    item_in.self = str(http_request.url)

    # Update the document and reset state for the new subscription parameters
    json_data = jsonable_encoder(item_in)
    json_data["state"] = jsonable_encoder(AnalyticsState())
    crud_mongo.update_new_field(db_mongo, db_collection, subscriptionId, json_data)

    # Retrieve the updated document | UpdateResult is not a dict
    updated_doc = crud_mongo.read_uuid(db_mongo, db_collection, subscriptionId)
    updated_doc.pop("owner_id")
    updated_doc.pop("state")

    task, db_event = subscription_task_registry.get(subscriptionId)
    if not task.done() and db_event is not None:
        db_event.set()

    logger.info("Updated subscription %s for user %s", subscriptionId, current_user.id)
    http_response = JSONResponse(content=updated_doc, status_code=200)
    add_notifications(http_request, http_response, False)
    return http_response


@router.get(
    "/{afId}/subscriptions/{subscriptionId}",
    response_model=schemas.MonitoringEventSubscription,
)
def read_subscription(
    *,
    afId: str = Path(
        ...,
        title="The ID of the Netapp that creates a subscription",
        example="myNetapp",
    ),
    subscriptionId: str = Path(..., title="Identifier of the subscription resource"),
    current_user: models.User = Depends(deps.get_current_active_user),
    http_request: Request,
) -> Any:
    """
    Get subscription by id
    """
    db_mongo = client.fastapi

    try:
        retrieved_doc = crud_mongo.read_uuid(db_mongo, db_collection, subscriptionId)
    except Exception as ex:
        raise HTTPException(
            status_code=400,
            detail="Please enter a valid uuid (24-character hex string)",
        )

    # Check if the document exists
    if not retrieved_doc:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # If the document exists then validate the owner
    if not user.is_superuser(current_user) and (
        retrieved_doc["owner_id"] != current_user.id
    ):
        raise HTTPException(status_code=400, detail="Not enough permissions")

    retrieved_doc.pop("owner_id")
    logger.info("Read subscription %s for user %s", subscriptionId, current_user.id)
    http_response = JSONResponse(content=retrieved_doc, status_code=200)
    add_notifications(http_request, http_response, False)
    return http_response


@router.delete(
    "/{afId}/subscriptions/{subscriptionId}",
    status_code=204
)
def delete_subscription(
    *,
    afId: str = Path(
        ...,
        title="The ID of the Netapp that creates a subscription",
        example="myNetapp",
    ),
    subscriptionId: str = Path(..., title="Identifier of the subscription resource"),
    current_user: models.User = Depends(deps.get_current_active_user),
    http_request: Request,
):
    """
    Delete a subscription
    """
    db_mongo = client.fastapi

    try:
        retrieved_doc = crud_mongo.read_uuid(db_mongo, db_collection, subscriptionId)
    except Exception as ex:
        raise HTTPException(
            status_code=400,
            detail="Please enter a valid uuid (24-character hex string)",
        )

    # Check if the document exists
    if not retrieved_doc:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # If the document exists then validate the owner
    if not user.is_superuser(current_user) and (
        retrieved_doc["owner_id"] != current_user.id
    ):
        raise HTTPException(status_code=400, detail="Not enough permissions")

    crud_mongo.delete_by_uuid(db_mongo, db_collection, subscriptionId)

    task, db_event = subscription_task_registry.get(subscriptionId)
    if not task.done() and db_event is not None:
        db_event.set()

    logger.info("Deleted subscription %s for user %s", subscriptionId, current_user.id)
    return Response(status_code=204)



@router.post(
    "/{afId}/fetch",
    response_model=schemas.analyticsExposure.AnalyticsData,
    responses={204: {"model": None}},
)
async def fetch_analytics(
    *,
    db: Session = Depends(deps.get_db),
    afId: str = Path(
        ...,
        title="The ID of the Netapp that is fetching analytics",
        example="myNetapp"
    ),
    item_in: schemas.analyticsExposure.AnalyticsRequest,
    current_user: models.User = Depends(deps.get_current_active_user),
    driver: AnalyticsExposureDep,
    http_request: Request,
) -> Any:
    # TODO check this later
    # NOTE: POST /{afId}/fetch/ should return a ProblemDetailsAnalyticsInfoRequest error (500) if the analytics request is
    # rejected by the NWDAF (this is N/A for now since we don't have an actual NWDAF)

    if item_in.analyEvent not in supported_subscription_events:
        raise HTTPException(
            status_code=501, detail=f"Analytics Event {item_in.analyEvent} has not been implemented"
        )

    if not (
        item_in.tgtUe
        and any([item_in.tgtUe.gpsi, item_in.tgtUe.anyUeInd, item_in.tgtUe.exterGroupId])
    ):
        raise HTTPException(status_code=404, detail="No Target UE Specified")

    if not get_subsc_ues(db, item_in, owner_id=current_user.id):
        raise HTTPException(status_code=404, detail=f"Device not found for tgtUe {item_in.tgtUe}")

    handler = handler_for_fetch(item_in.analyEvent, db_sql=db, driver=driver, analytics_request=item_in)
    if handler is None:
        raise HTTPException(status_code=500, detail="Could not prepare analytics request")

    result = await handler.get_analytics()
    if not result:
        # No Content (The requested Analytics data does not exist)
        return Response(status_code=204)

    http_response = JSONResponse(content=jsonable_encoder(result), status_code=200)
    add_notifications(http_request, http_response, False)
    return http_response

