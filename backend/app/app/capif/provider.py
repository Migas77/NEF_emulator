import logging
import os
import opencapif_sdk
import yaml
from fastapi import FastAPI, requests
from requests import RequestException

from app.capif import CAPIF_SDK_CONFIG_PATH

from app.core.config import settings


class CapifProvider:

    def __init__(self, config_path: str):
        self.config_path: str = config_path
        self.capif_cert_pem_path: str | None = None
        self.provider: opencapif_sdk.capif_provider_connector | None = None
        self.service_discoverer: opencapif_sdk.service_discoverer | None = None
        self.logging_feature: opencapif_sdk.capif_logging_feature | None = None
        self.base_url = f"https://{settings.NGINX_HOST}:{settings.NGINX_HTTPS}/nef/api/v1"
        self.api_name = "api"
        # api_name is extracted using translator.build() from the nef base url (https://<host>:<port>/<apiRoot>/<apiName>/v<version>)
        # It cannot be changed without changing the url

    def startup(self, nef_app: FastAPI, prefix: str = ""):
        logging.info(f"Starting capif provider")
        self.provider = opencapif_sdk.capif_provider_connector(config_file=self.config_path)
        is_onboarded = self.__is_onboarded()

        logging.info(f"CAPIF provider is onboarded: {is_onboarded}")
        if is_onboarded:
            self.capif_cert_pem_path = os.path.join(self.provider.provider_folder, "capif_cert_server.pem")
            self.logging_feature = opencapif_sdk.capif_logging_feature(config_file=self.config_path)
            logging.info("Skipping CAPIF provider onboarding and service publishing.")
            return

        # Onboard new provider
        self.provider.onboard_provider()
        self.capif_cert_pem_path = os.path.join(self.provider.provider_folder, "capif_cert_server.pem")
        logging.info("CAPIF provider onboarded.")

        # Get NEF OpenAPI schema and translate to CAPIF format
        openapi_schema_name = "nef_openapi"
        openapi_schema_spec = nef_app.openapi()
        openapi_schema_spec["paths"] = {
            f"{prefix}{path}": value
            for path, value in openapi_schema_spec["paths"].items()
        }
        with open(f"{openapi_schema_name}.yaml", "w") as f:
            yaml.dump(openapi_schema_spec, f, allow_unicode=True, sort_keys=False)
        translator = opencapif_sdk.api_schema_translator(f"./{openapi_schema_name}.yaml")
        translator.build(
            url=self.base_url,
            supported_features="0", api_supp_features="0"
        )
        self.provider.api_description_path=f"./{self.api_name}.json"
        self.__publish_services()

        # capif_logging_feature reads provider_service_ids.json, so it can only be built
        # after publishing. Without this it is only set on the already onboarded branch
        # above, leaving it None for the whole life of the process that onboards for the
        # first time - and with it, no invocation logging to CAPIF until a restart.
        self.logging_feature = opencapif_sdk.capif_logging_feature(config_file=self.config_path)


    def shutdown(self):
        logging.info("Shutting down CAPIF provider...")

        if settings.CAPIF_CLEANUP_ON_SHUTDOWN and self.provider and self.__is_onboarded():
            logging.info("Offboarding CAPIF provider and unpublishing services...")
            self.provider.publish_req['service_api_id'] = self.provider.provider_service_ids[self.api_name]
            self.provider.publish_req['publisher_apf_id'] = self.provider.provider_capif_ids['APF-1']
            self.provider.publish_req['publisher_aefs_ids'] = [self.provider.provider_capif_ids['AEF-1']]
            self.provider.unpublish_service()
            self.provider.offboard_provider()
            logging.info("CAPIF provider offboarded.")

    def __publish_services(self):
        logging.info("Publishing nef services to CAPIF...")
        APF1 = self.provider.provider_capif_ids['APF-1']
        AEF1 = self.provider.provider_capif_ids['AEF-1']
        self.provider.publish_req['publisher_apf_id'] = APF1
        self.provider.publish_req['publisher_aefs_ids'] = [AEF1]
        try:
            self.provider.publish_services()
        except RequestException as e:
            logging.warning(f"SDK Request to CAPIF failed: {e} - Response: {e.response.text}")
            details = e.response.json()
            if details.get("status") != 403 or details.get("detail") != "Already registered service with same api name":
                logging.error("Unexpected error during service publication. Terminating...")
                raise

    def __is_onboarded(self) -> bool:
        return bool(self.provider is not None and self.provider.provider_capif_ids)


capif_provider = CapifProvider(config_path=CAPIF_SDK_CONFIG_PATH)

