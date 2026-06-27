import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import models
from app.api import deps
from app.crud import ue as crud_ue
from app.drivers.sms import SMSInterfaceDep
from app.schemas.sms import SMSSendRequest, SMSSendResponse

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/send", response_model=SMSSendResponse, status_code=200)
async def send_sms(
    body: SMSSendRequest,
    sms_interface: SMSInterfaceDep,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> SMSSendResponse:
    ue = None

    if body.gpsi is not None:
        raw_gpsi = body.gpsi.__root__
        if raw_gpsi.startswith("msisdn-"):
            ue = crud_ue.get_msisdn(db, msisdn=raw_gpsi.removeprefix("msisdn-"))
        elif raw_gpsi.startswith("extid-"):
            ue = crud_ue.get_externalId(db, externalId=raw_gpsi.removeprefix("extid-"), owner_id=current_user.id)

    elif body.supi is not None:
        ue = crud_ue.get_supi(db, supi=body.supi.__root__)

    if ue is None:
        raise HTTPException(status_code=404, detail="UE not found")

    if not ue.msisdn:
        raise HTTPException(status_code=500, detail="UE has no MSISDN configured. Could not send SMS.")

    try:
        await sms_interface.send_sms(ue.msisdn, body.text)
    except Exception as e:
        logger.exception("Failed to send SMS to msisdn=%s", ue.msisdn)
        raise HTTPException(status_code=500, detail="Failed to send SMS") from e

    return SMSSendResponse(msisdn=ue.msisdn)