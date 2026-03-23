from enum import Enum
from pydantic import BaseModel


# Based on webhook alert payload provided by Prometheus Alertmanager
# https://prometheus.io/docs/alerting/latest/configuration/#webhook_config


class AlertStatus(str, Enum):
    firing = "firing"
    resolved = "resolved"


class Alert(BaseModel):
    status: AlertStatus
    labels: dict[str, str]
    annotations: dict[str, str]
    startsAt: str
    endsAt: str
    generatorURL: str
    fingerprint: str


class AlertNotification(BaseModel):
    version: str
    groupKey: str
    truncatedAlerts: int
    status: AlertStatus
    receiver: str
    groupLabels: dict[str, str]
    commonLabels: dict[str, str]
    externalURL: str
    alerts: list[Alert]

