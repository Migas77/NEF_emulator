from typing import List, Optional
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.models import IMSIGroup_UE_membership, IMSIGroup, ExternalGroup
from app.models.UE import UE
from app.schemas.UE import UECreate, UEUpdate


class CRUD_UE(CRUDBase[UE, UECreate, UEUpdate]):
    def create_with_owner(self, db: Session, *, obj_in: UECreate, owner_id: int) -> UE:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data, owner_id=owner_id)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def get_multi_by_owner(
        self, db: Session, *, owner_id: int, skip: int = 0, limit: int = 100
    ) -> List[UE]:
        return (
            db.query(self.model)
            .filter(UE.owner_id == owner_id)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_supi(self, db: Session, supi: str) -> Optional[UE]:
        return db.query(self.model).filter(self.model.supi == supi).first()

    def get_supi_multi(self, db: Session, supis: List[str]) -> List[UE]:
        return db.query(self.model).filter(self.model.supi.in_(supis)).all()

    def get_ipv4(self, db: Session, *, ipv4: str, owner_id: int) -> Optional[UE]:
        return (
            db.query(self.model)
            .filter(UE.ip_address_v4 == ipv4, UE.owner_id == owner_id)
            .first()
        )

    def get_ipv6(self, db: Session, *, ipv6: str, owner_id: int) -> Optional[UE]:
        return (
            db.query(self.model)
            .filter(UE.ip_address_v6 == ipv6, UE.owner_id == owner_id)
            .first()
        )

    def get_mac(self, db: Session, *, mac: str, owner_id: int) -> Optional[UE]:
        return (
            db.query(self.model)
            .filter(UE.mac_address == mac, UE.owner_id == owner_id)
            .first()
        )

    def get_msisdn(self, db: Session, *, msisdn: str, owner_id: int) -> Optional[UE]:
        return (
            db.query(self.model)
            .filter(UE.msisdn == msisdn, UE.owner_id == owner_id)
            .first()
        )

    def get_externalId(
        self, db: Session, *, externalId: str, owner_id: int | None = None
    ) -> Optional[UE]:
        opt_owner_filter = ((UE.owner_id == owner_id,) if owner_id is not None else ())
        return (
            db.query(self.model)
            .filter(UE.external_identifier == externalId, *opt_owner_filter)
            .first()
        )

    def get_by_exterGroupId(
        self, db: Session, *, exterGroupId: str
    ) -> Optional[UE]:
        return (
            db.query(UE)
            .join(IMSIGroup_UE_membership, UE.id == IMSIGroup_UE_membership.c.ue_id)
            .join(ExternalGroup, ExternalGroup.imsiGroupId == IMSIGroup_UE_membership.c.imsiGroupId)
            .filter(ExternalGroup.exterGroupId == exterGroupId)
            .all()
        )

    def get_by_Cell(self, db: Session, *, cell_id: int) -> List[UE]:
        return db.query(self.model).filter(UE.Cell_id == cell_id).all()

    def update_coordinates(
        self, db: Session, *, lat: float, long: float, db_obj: UE
    ) -> UE:
        setattr(db_obj, "latitude", lat)
        setattr(db_obj, "longitude", long)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def remove_supi(self, db: Session, *, supi: str) -> UE:
        print(f'"removing supi"{supi}')
        obj = db.query(self.model).filter(UE.supi == supi).first()
        print("Done")
        print(obj)
        db.delete(obj)
        db.commit()
        return obj


ue = CRUD_UE(UE)
