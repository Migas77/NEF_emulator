import logging
from collections.abc import Collection
from datetime import timedelta, datetime, timezone
from typing import Any
from sqlalchemy.orm import Session
from app import schemas
from app.core.notification_responder import notification_responder
from app.crud import ue as crud_ue
from app.schemas.commonData import BitRate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bps_to_xbps_bitrate(bitrate_value: str) -> BitRate:
    units = {"Tbps": 1e12, "Gbps": 1e9, "Mbps": 1e6, "Kbps": 1e3}
    value = float(bitrate_value)
    for unit, threshold in units.items():
        if value >= threshold:
            return f"{value / threshold:.2f} {unit}"
    return f"{value:.2f} bps"


async def send_notification(notif_uri: str, json: Any, *, subscription_id: str, event: str) -> bool:
    try:
        await notification_responder.send_notification(notif_uri, json)
        logger.info("Notification sent [sub=%s, event=%s] to %s", subscription_id, event, notif_uri)
        return True
    except Exception as e:
        logger.error("Error sending notification [sub=%s, event=%s]: %s", subscription_id, event, e)
        return False


def get_subsc_ues(
    db: Session,
    s: schemas.AnalyticsEventSubsc | schemas.analyticsExposure.AnalyticsRequest,
    *,
    owner_id: int | None = None
) -> list[schemas.UE]:
    user_equipments = []
    tgt_ue = s.tgtUe

    try:
        if tgt_ue:
            if tgt_ue.gpsi:
                gpsi = tgt_ue.gpsi
                if gpsi.startswith("msisdn-"):
                    ue = crud_ue.get_msisdn(db, msisdn=gpsi.split("msisdn-")[1])
                    if ue is not None:
                        user_equipments = [ue]

                elif gpsi.startswith("extid-"):
                    ue = crud_ue.get_externalId(
                        db, externalId=gpsi.split("extid-")[1],
                        **({"owner_id": owner_id} if owner_id is not None else {}),
                    )
                    if ue is not None:
                        user_equipments = [ue]

            elif tgt_ue.exterGroupId:
                user_equipments = crud_ue.get_by_exterGroupId(db, exterGroupId=tgt_ue.exterGroupId)

            elif tgt_ue.anyUeInd:
                user_equipments = crud_ue.get_all(db)

    except Exception as e:
        logger.exception("Error looking up UEs for tgtUe %s", tgt_ue)

    if not user_equipments:
        logger.warning("No UEs found for tgtUe %s", tgt_ue)

    return user_equipments


def population_variance(values: Collection[float]) -> float | None:
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    return sum((x - mean) ** 2 for x in values) / n

def compute_mean(values: Collection) -> float:
    return sum(values) / len(values)


def subscription_id_from_link(link: str) -> str:
    return link.rstrip("/").rsplit("/", 1)[-1]


def resolve_query_start_end_ts(
    offset_period: timedelta | None = None,     # offset period to the reporting time (only if "repPeriod" is included)
    rep_period: timedelta | None = None,        # reporting period (only if PERIODIC notification method)
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    # Based on 3GPP TS 29.520:
    # - offsetPeriod and startTs/endTs are mutually exclusive;
    # - offsetPeriod=0 or startTs==endTs signals a point-in-time request rather than a time interval
    # - If offsetPeriod is negative, it refers to a past offset period (historical statistics);
    #   if positive, it refers to a future offset period (prediction). [Note: here only historical stats are supported]
    # - If startTs is absent, the observation period starts at the present time (unless offsetPeriod is included).
    # - If startTs is in the past and endTs is absent, the end time defaults to the present time (unless offsetPeriod is included).
    # - If none of startTs, endTs, or offsetPeriod is provided, the target period starts
    #   at the present time with no specified end.
    now = datetime.now(timezone.utc)

    if offset_period is not None:
        if rep_period is None:
            logger.error("offsetPeriod may be present only if repPeriod is included")
            return None
        if offset_period > timedelta(0):
            logger.error("Positive offset period (prediction) is not implemented")
            return None
        return now + offset_period, now

    if start_ts is not None:
        return start_ts, end_ts or now

    if end_ts is not None:
        logger.error("endTs provided without startTs is not defined by the spec")
        return None

    logger.error("No temporal bounds provided: open-ended periods are not supported for historical analytics")
    return None

