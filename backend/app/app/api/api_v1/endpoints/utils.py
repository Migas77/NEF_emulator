import os
import json
import logging
import requests
from datetime import datetime
from json import JSONDecodeError

from typing import Callable, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException, RequestValidationError
from pydantic import BaseModel
from fastapi.routing import APIRoute

from app import models
from app.api import deps
from app.schemas import monitoringevent, resourceManagementOfBdt
from app.schemas.afSessionWithQos import UserPlaneNotificationData
from app.core.config import settings
from app.schemas.commonData import SupportedFeatures

#List holding notifications from 
event_notifications = []
counter = 0

logs_count = 0

def add_notifications(
    request: Request,
    response: Optional[JSONResponse],
    is_notification: bool,
    *,
    # The status code if no response was provided
    status_code: int = 204
):
    global counter

    json_data = {}
    json_data.update({"id" : counter})

    #Find the service API 
    #Keep in mind that whether endpoint changes format, the following if statement needs review
    #Since new APIs are added in the emulator, the if statement will expand
    endpoint = request.url.path
    if endpoint.find('monitoring') != -1:
        serviceAPI = "Monitoring Event API"
    elif endpoint.find('session-with-qos') != -1:
        serviceAPI = "AsSession With QoS API"
    elif endpoint.find('qosInfo') != -1:
        serviceAPI = "QoS Information"
    elif endpoint.find('bdt') != -1:
        serviceAPI = "Resource Management Of Bdt API"
    elif endpoint.find('traffic-influence') != -1:
        serviceAPI = "Traffic Influence API"
    elif endpoint.find('chargeable-party') != -1:
        serviceAPI = "Chargeable Party API"
    elif endpoint.find('net-stat-report') != -1:
        serviceAPI = "Reporting Network Status API"
    elif endpoint.find('cp-parameter-provisioning') != -1:
        serviceAPI = "Cp Parameter Provisioning API"
    elif endpoint.find('analyticsexposure') != -1:
        serviceAPI = "Analytics Exposure API"

    #Request body check and trim
    if(request.method == 'POST') or (request.method == 'PUT'):  
        req_body = request._body.decode("utf-8").replace('\n', '')
        req_body = req_body.replace(' ', '')
        json_data["request_body"] = req_body

    json_data["response_body"] = response.body.decode("utf-8") if response is not None else ""
    json_data["endpoint"] = endpoint
    json_data["serviceAPI"] = serviceAPI
    json_data["method"] = request.method    
    json_data["status_code"] = response.status_code if response is not None else status_code
    json_data["isNotification"] = is_notification
    json_data["timestamp"] = datetime.now()

    #Check that event_notifications length does not exceed 100
    event_notifications.append(json_data)
    if len(event_notifications) > 100:
        event_notifications.pop(0)

    counter += 1

    return json_data
    
class callback(BaseModel):
    callbackurl: str

router = APIRouter()

@router.post("/test/callback")
def get_test(
    item_in: callback
    ):
    
    callbackurl = item_in.callbackurl
    print(callbackurl)
    payload = json.dumps({
    "externalId" : "10000@domain.com",
    "ipv4Addr" : "10.0.0.0",
    "subscription" : "http://localhost:8888/api/v1/3gpp-monitoring-event/v1/myNetapp/subscriptions/whatever",
    "monitoringType": "LOCATION_REPORTING",
    "locationInfo": {
        "cellId": "AAAAAAAAA",
        "enodeBId": "AAAAAA"
    }
    })

    headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", callbackurl, headers=headers, data=payload)
        return response.json()
    except requests.exceptions.ConnectionError as ex:
        logging.warning(ex)
        raise HTTPException(status_code=409, detail=f"Failed to send the callback request. Error: {ex}")

@router.post("/session-with-qos/callback")
def create_item(item: UserPlaneNotificationData, request: Request):

    http_response = JSONResponse(content={'ack' : 'TRUE'}, status_code=200)
    add_notifications(request, http_response, True)
    return http_response 

@router.post("/monitoring/callback")
def create_item(item: monitoringevent.MonitoringNotification, request: Request):

    http_response = JSONResponse(content={'ack' : 'TRUE'}, status_code=200)
    add_notifications(request, http_response, True)
    return http_response 

@router.post("/bdt/callback")
def create_item(item: resourceManagementOfBdt.ExNotification, request: Request):

    http_response = JSONResponse(content={'ack' : 'TRUE'}, status_code=200)
    add_notifications(request, http_response, True)
    return http_response 

@router.get("/monitoring/notifications")
def get_notifications(
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(deps.get_current_active_user)
    ):
    notification = event_notifications[skip:limit]
    return notification

@router.get("/monitoring/last_notifications")
def get_last_notifications(
    id: int = Query(..., description="The id of the last retrieved item"),
    current_user: models.User = Depends(deps.get_current_active_user)
    ):
    updated_notification = []
    event_notifications_snapshot = event_notifications


    if id == -1:
        return event_notifications_snapshot

    if event_notifications_snapshot:
        if event_notifications_snapshot[0].get('id') > id:
            return event_notifications_snapshot
    else:
        raise HTTPException(status_code=409, detail="Event notification list is empty")
            
    skipped_items = 0


    for notification in event_notifications_snapshot:
        if notification.get('id') == id:
            updated_notification = event_notifications_snapshot[(skipped_items+1):]
            break
        skipped_items += 1
    
    return updated_notification

class ReportLogging(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:

            try:
                request_body = {}
                try:
                    request_body = await request.json()
                except JSONDecodeError:
                    pass

                response = await original_route_handler(request)

                extra_fields = {
                    'endpoint': request.url.path,
                    'method': request.method,
                    'request_body': request_body,
                    **self.get_query_params(request.query_params),
                    'nef_response_code': response.status_code,
                    'nef_response_message': response.body.decode(response.charset).replace('"', "'"),
                }

                self.update_log_file(extra_fields)

                return response
            except (RequestValidationError, HTTPException) as exc:
                status_code = 422 if isinstance(exc, RequestValidationError) else exc.status_code

                extra_fields = {
                    'endpoint': request.url.path,
                    'method': request.method,
                    'request_body': exc.body if isinstance(exc, RequestValidationError) else request_body,
                    **self.get_query_params(request.query_params),
                    'nef_response_code': status_code,
                    'nef_response_message': exc.detail if isinstance(exc, HTTPException) else exc.errors(),
                }

                self.update_log_file(extra_fields)

                raise HTTPException(status_code=status_code, detail=exc.detail if isinstance(exc, HTTPException) else exc.errors())

        return custom_route_handler

    def write_log_file(self, log_entry):
        try:
            # Use os.open directly to define flags instead of open("a") because
            # it doesn't create the file.
            with os.fdopen(os.open(settings.REPORT_PATH, os.O_APPEND | os.O_WRONLY), "a") as json_file:
                json_file.write("\x1E")
                json.dump(log_entry, json_file, indent=None)
                json_file.write("\n")
        except FileNotFoundError:
            # If the report file doesn't exist ignore errors
            pass

    def update_log_file(self, extra_fields):
        global logs_count

        logs_count += 1

        log_entry = {
            'id': logs_count,
            **extra_fields
        }

        self.write_log_file(log_entry)

    def get_query_params(self, request_query_params):
        query_params = {
            'scsAsId': None,
            'afId': None,
            'subscriptionId': None,
            'transactionId': None,
            'configurationId': None,
            'provisioningId': None,
            'setId': None,
        }

        for param_name in query_params:
            if param_name in request_query_params:
                query_params[param_name] = request_query_params[param_name]

        return query_params

def decode_supported_features(supported_features: SupportedFeatures) -> int:
    # Pad the string with an extra 0 if necessary for hex processing
    if len(supported_features) % 2 != 0:
        supported_features = "0" + supported_features

    hex_bytes = bytes.fromhex(supported_features)
    return int.from_bytes(hex_bytes, byteorder="big")

def encode_supported_features(supported_features: int) -> SupportedFeatures:
    hex_bytes_length = (supported_features.bit_length() + 7) // 8
    hex_bytes = supported_features.to_bytes(hex_bytes_length, byteorder='big')
    return hex_bytes.hex()
