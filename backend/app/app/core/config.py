import json
import secrets
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from typing_extensions import Annotated

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    BaseSettings,
    EmailStr,
    Field,
    HttpUrl,
    PostgresDsn,
    confloat,
    conint,
    parse_obj_as,
    validator,
)


class AnalyticsExposureBackend(Enum):
    STUB = "stub"
    PROMETHEUS = "prometheus"


class AnalyticsExposureInterfaceSettings(BaseModel):
    backend: AnalyticsExposureBackend = AnalyticsExposureBackend.STUB
    url: str = ""
    poll_interval: confloat(gt=0) = 10.0
    temporal_gran_size: conint(ge=1) = 60
    offset_period: Optional[conint(le=-1)] = None

    @validator("url", always=True)
    def validate_url(cls, v, values):
        if not v and values["backend"] == AnalyticsExposureBackend.PROMETHEUS:
            raise ValueError("url must be set when prometheus analytics exposure backend is in use")
        return v

    @validator("offset_period", always=True)
    def default_offset_period(cls, v, values):
        if v is None:
            return -values.get("temporal_gran_size", 60)
        return v


class SMSBackend(Enum):
    MOCK = "mock"
    SMSC = "smsc"


class SMSInterfaceSettings(BaseModel):
    backend: SMSBackend = SMSBackend.MOCK
    smsc_url: str = ""
    sender_id: str = ""

    @validator("smsc_url", "sender_id", always=True)
    def validate_smsc_set(cls, v, field, values):
        if not v and values.get("backend") == SMSBackend.SMSC:
            raise ValueError(f"{field.name} must be set when smsc SMS backend is in use")
        return v


class QoSInterfaceBackend(Enum):
    NOOP = "noop"
    HUWAEI = "huwaei"


class QoSInterfaceSettings(BaseModel):
    backend: QoSInterfaceBackend = QoSInterfaceBackend.NOOP

    huwaei_api_url: str = ""
    huwaei_api_user: str = ""
    huwaei_api_password: str = ""
    huwaei_default_ambrup: int = 0
    huwaei_default_ambrdl: int = 0

    @validator(
        "huwaei_api_url",
        "huwaei_api_user",
        "huwaei_api_password",
        "huwaei_default_ambrup",
        "huwaei_default_ambrdl",
        always=True,
    )
    def validate_huwaei_set(cls, v, field, values):
        if not v and values["backend"] == QoSInterfaceBackend.HUWAEI:
            raise ValueError(
                f"{field.name} must be set when huwaei QoS backend is in use"
            )
        return v


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    SERVER_NAME: str
    SERVER_HOST: AnyHttpUrl
    # BACKEND_CORS_ORIGINS is a JSON-formatted list of origins
    # e.g: '["http://localhost", "http://localhost:4200", "http://localhost:3000", \
    # "http://localhost:8080", "http://local.dockertoolbox.tiangolo.com"]'
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    PROJECT_NAME: str
    SENTRY_DSN: Optional[HttpUrl] = None

    @validator("SENTRY_DSN", pre=True)
    def sentry_dsn_can_be_blank(cls, v: str) -> Optional[str]:
        if len(v) == 0:
            return None
        return v

    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            path=f"/{values.get('POSTGRES_DB') or ''}",
        )

    # MONGO_CLIENT: str

    NGINX_HOST: str
    NGINX_HTTPS: str

    # CAPIF settings (required when CAPIF_ENABLED=True)
    CAPIF_ENABLED: bool = False
    CAPIF_CLEANUP_ON_SHUTDOWN: bool = False
    CAPIF_HOST: Optional[str] = None
    REGISTER_HOST: Optional[str] = None
    CAPIF_HTTPS_PORT: Optional[str] = None
    CAPIF_REGISTER_PORT: Optional[str] = None
    CAPIF_USERNAME: Optional[str] = None
    CAPIF_PASSWORD: Optional[str] = None

    @validator(
        "CAPIF_HOST",
        "REGISTER_HOST",
        "CAPIF_HTTPS_PORT",
        "CAPIF_REGISTER_PORT",
        "CAPIF_USERNAME",
        "CAPIF_PASSWORD",
        always=True,
    )
    def validate_capif_settings(cls, v, field, values):
        if not v and values.get("CAPIF_ENABLED"):
            raise ValueError(
                f"{field.name} must be set when CAPIF_ENABLED is true"
            )
        return v

    SMTP_TLS: bool = True
    SMTP_PORT: Optional[int] = None
    SMTP_HOST: Optional[str] = None
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAILS_FROM_EMAIL: Optional[EmailStr] = None
    EMAILS_FROM_NAME: Optional[str] = None

    @validator("EMAILS_FROM_NAME")
    def get_project_name(cls, v: Optional[str], values: Dict[str, Any]) -> str:
        if not v:
            return values["PROJECT_NAME"]
        return v

    EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = 48
    EMAIL_TEMPLATES_DIR: str = "/app/app/email-templates/build"
    EMAILS_ENABLED: bool = False

    @validator("EMAILS_ENABLED", pre=True)
    def get_emails_enabled(cls, v: bool, values: Dict[str, Any]) -> bool:
        return bool(
            values.get("SMTP_HOST")
            and values.get("SMTP_PORT")
            and values.get("EMAILS_FROM_EMAIL")
        )

    # EMAIL_TEST_USER: EmailStr = "test@example.com"  # type: ignore
    FIRST_SUPERUSER: EmailStr
    FIRST_SUPERUSER_PASSWORD: str
    USERS_OPEN_REGISTRATION: bool = False

    REPORT_PATH: str

    qos: QoSInterfaceSettings = QoSInterfaceSettings()
    analyticsExposure: AnalyticsExposureInterfaceSettings = AnalyticsExposureInterfaceSettings()
    sms: SMSInterfaceSettings = SMSInterfaceSettings()

    class Config:
        # case_sensitive = True
        env_nested_delimiter = "__"


settings = Settings()


class QoSProfile(BaseModel):
    uplinkBitRate: Annotated[
        Optional[int], Field(description="Uplink bandwidth in bps")
    ] = None
    downlinkBitRate: Annotated[
        Optional[int], Field(description="Downlink bandwidth in bps")
    ] = None
    packetDelayBudget: Annotated[
        Optional[int], Field(description="Packet delay budget in milliseconds")
    ] = None
    packerErrRate: Annotated[
        Optional[str],
        Field(description="Packet error rate in exponential form", examples=["4E-2"]),
    ] = None


class NamedQoSProfile(QoSProfile):
    name: str


class QoSSettings:
    _qos_characteristics: dict[str, QoSProfile]

    def __init__(self) -> None:
        self.import_json()

    def import_json(self):
        with open("app/core/config/qosCharacteristics.json") as json_file:
            data = json.load(json_file)
            self._qos_characteristics = parse_obj_as(dict[str, QoSProfile], data)

    def get_all_profiles(self) -> List[NamedQoSProfile]:
        return [NamedQoSProfile(name=k, **v.dict()) for k, v in self._qos_characteristics.items()]

    def get_qos_profile(self, reference: str) -> Optional[QoSProfile]:
        return self._qos_characteristics.get(reference)


qosSettings = QoSSettings()
