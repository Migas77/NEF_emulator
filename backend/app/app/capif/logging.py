import logging
import os

import opencapif_sdk
import requests

from fastapi import Request, HTTPException, FastAPI
from fastapi.exception_handlers import http_exception_handler
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.capif.provider import capif_provider
from app.core.config import settings


def __create_logs(capif_logging_feature: opencapif_sdk.capif_logging_feature, log_entry: dict, aefId, jwt, supp_features="0"):
    """
    opencapif_sdk.capif_logging_feature.create_logs function receives the log entry via a shared logging_feature.log dict
    this doesn't make any sense as this log object would be shared and overwritten by concurrent requests
    therefore this function is a mere copy of that function, with the log entry being passed as a parameter instead
    """

    api_invoker_id = capif_logging_feature._decrypt_jwt(jwt)

    path = capif_logging_feature.capif_https_url + f"/api-invocation-logs/v1/{aefId}/logs"

    payload = {
        "aefId": f"{aefId}",
        "apiInvokerId": f"{api_invoker_id}",
        "logs": [log_entry],
        "supportedFeatures": f"{supp_features}"
    }
    provider_details = capif_logging_feature._capif_logging_feature__load_provider_api_details()
    AEF_api_prov_func_id = aefId
    aef_number = None
    for key, value in provider_details.items():
        if value == AEF_api_prov_func_id and key.startswith("AEF-"):
            aef_inter = key.split("-")[1]
            # Obtain the aef number
            aef_number = aef_inter.split("_")[0]
            break

    if aef_number is None:
        capif_logging_feature.logger.error(
            f"No matching AEF found for publisher_aef_id: {AEF_api_prov_func_id}")
        raise ValueError("Invalid publisher_aef_id")

    cert = (
        os.path.join(capif_logging_feature.provider_folder, f"aef-{aef_number}.crt"),
        os.path.join(capif_logging_feature.provider_folder,
                     f"AEF-{aef_number}_private_key.key"),
    )

    try:
        response = requests.post(
            url=path,
            json=payload,
            timeout=10,
            headers={"Content-Type": "application/json"},
            cert=cert,
            verify=os.path.join(capif_logging_feature.provider_folder, "ca.crt")
        )

        response.raise_for_status()

        return response.status_code, response.json()

    except Exception as e:
        capif_logging_feature.logger.error("Unexpected error: %s",e)
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            capif_logging_feature.logger.error(
                "CAPIF response body: %s", e.response.text
            )
        return None, {"error": f"Unexpected error: {e}"}

def log_to_capif(log_entry: dict, jwt):
    if not settings.CAPIF_ENABLED:
        return

    logging_feature = capif_provider.logging_feature
    if logging_feature is None:
        logging.error("CAPIF logging feature is not initialized but CAPIF_ENABLED is true.")
        return

    # Populate the log entry with the required information
    log_entry = {
        "apiId": logging_feature.api_id,
        "apiName": logging_feature.log["apiName"],
        "apiVersion": log_entry["apiVersion"],
        "resourceName": log_entry["resourceName"],
        "uri": log_entry["uri"],
        "protocol": log_entry["protocol"],
        "operation": log_entry["operation"],
        "result": log_entry["result"]
    }
    aefId = logging_feature.provider_capif_ids.get('AEF-1')
    if aefId is None:
        logging.error("AEF-1 ID not found in provider_capif_ids.")
        return
    __create_logs(logging_feature, log_entry, aefId, jwt)


class CAPIFLoggingMiddleware(BaseHTTPMiddleware):

    def __init__(self, app: FastAPI):
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
        except HTTPException as exc:
            response = await http_exception_handler(request, exc)
            status_code = response.status_code
        finally:
            auth_header = request.headers.get("Authorization", "")
            jwt = auth_header.removeprefix("Bearer ") if auth_header.startswith("Bearer ") else None

            if jwt is not None:
                log_entry = {
                    "apiVersion": settings.API_V1_STR,
                    "resourceName": request.url.path.split("/")[-1],
                    "uri": str(request.url),
                    "protocol": "HTTP_1_1",
                    "operation": request.method,
                    "result": str(status_code),
                }
                log_to_capif(log_entry, jwt)

        return response

