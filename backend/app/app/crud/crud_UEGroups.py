from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.models import IMSIGroup, ExternalGroup
from app.schemas import IMSIGroupCreate
from app.schemas.UEGroups import IMSIGroupUpdate, ExternalGroupCreate, ExternalGroupUpdate


class CRUD_IMSIGroup(CRUDBase[IMSIGroup, IMSIGroupCreate, IMSIGroupUpdate]):

    def create_IMSIGroup_with_UEs(self, db: Session, *, obj_in: IMSIGroupCreate, UEs: list) -> IMSIGroup:
        obj_in_data = jsonable_encoder(obj_in, exclude={"UEs"})
        db_obj = self.model(**obj_in_data, UEs=UEs)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj


class CRUD_ExternalGroup(CRUDBase[ExternalGroup, ExternalGroupCreate, ExternalGroupUpdate]):
    pass



imsi_group = CRUD_IMSIGroup(IMSIGroup)
external_group = CRUD_ExternalGroup(ExternalGroup)

