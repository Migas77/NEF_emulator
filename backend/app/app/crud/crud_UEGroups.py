import logging
from typing import Optional
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
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

    def create_if_not_exists(self, db: Session, *, obj_in: ExternalGroupCreate) -> Optional[ExternalGroup]:
        try:
            is_present = self.get_by_exterGroupId(db=db, exterGroupId=obj_in.exterGroupId)
            if is_present:
                return None

            return self.create(db=db, obj_in=obj_in)
        except IntegrityError as e:
            # prevent TOCTOU problem:
            logging.warning(f"Unique violation detected for {obj_in}: {e.orig}")
            db.rollback()
            return None

    def get_by_exterGroupId(self, db: Session, *, exterGroupId: str) -> Optional[ExternalGroup]:
        return db.query(self.model).filter_by(exterGroupId=exterGroupId).first()


imsi_group = CRUD_IMSIGroup(IMSIGroup)
external_group = CRUD_ExternalGroup(ExternalGroup)

