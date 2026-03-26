from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from requests import Session
from sqlalchemy.testing import exclude

from app import crud, models, schemas
from app.crud import ue as crud_ue, external_group as crud_external_group, imsi_group as crud_imsi_group
from app.api import deps
from app.api.api_v1.endpoints.utils import ReportLogging

router = APIRouter()
router.route_class = ReportLogging


@router.post("/imsiGroup", response_model=schemas.IMSIGroup)
def create_imsi_group_identifier(
    *,
    db: Session = Depends(deps.get_db),
    item_in: schemas.IMSIGroupCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
):
    ues = crud_ue.get_supi_multi(db=db, supis=jsonable_encoder(item_in.UEs))
    if not ues:
        raise HTTPException(status_code=404, detail="UEs not found")
    if len(ues) != len(item_in.UEs):
        not_found_supis = set(item_in.UEs) - set(ue.supi for ue in ues)
        raise HTTPException(status_code=404, detail=f"UEs with ids {not_found_supis} not found")

    imsi_group = crud_imsi_group.create_IMSIGroup_with_UEs(db=db, obj_in=item_in, UEs=ues)

    return {
        "id": imsi_group.id,
        "UEs": [ue.supi for ue in imsi_group.UEs]
    }


@router.post("/exterGroup", response_model=schemas.ExternalGroup)
def create_external_group_identifier(
        *,
        db: Session = Depends(deps.get_db),
        item_in: schemas.ExternalGroupCreate,
        current_user: models.User = Depends(deps.get_current_active_user),
) -> schemas.ExternalGroupBase:
    imsi_group = crud_imsi_group.get(db=db, id=item_in.imsiGroupId)
    if not imsi_group:
        raise HTTPException(status_code=404, detail="IMSI Group not found")

    external_group = crud_external_group.create(db=db, obj_in=item_in)
    return external_group








