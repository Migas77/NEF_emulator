from typing import Any, Dict, List, Union

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.models.application import Application
from app.schemas.application import ApplicationCreate, ApplicationUpdate


class CRUDApplication(CRUDBase[Application, ApplicationCreate, ApplicationUpdate]):

    def create_with_owner(self, db: Session, *, obj_in: ApplicationCreate, owner_id: int) -> Application:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data, owner_id=owner_id)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def get_multi_by_owner(
        self, db: Session, *, owner_id: int, skip: int = 0, limit: int = 100
    ) -> List[Application]:
        return (
            db.query(self.model)
            .filter(Application.owner_id == owner_id)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_by_app_id(self, db: Session, *, app_id: str) -> Application | None:
        return db.query(self.model).filter(self.model.app_id == app_id).first()

    def get_apps_by_app_ids(self, db: Session, *, app_ids: list[str]) -> list[Application]:
        return db.query(self.model).filter(self.model.app_id.in_(app_ids)).all()    # noqa

    def update(
        self,
        db: Session,
        *,
        db_obj: Application,
        obj_in: Union[ApplicationUpdate, Dict[str, Any]],
    ) -> Application:
        if not isinstance(obj_in, dict):
            obj_in = jsonable_encoder(obj_in)
        return super().update(db=db, db_obj=db_obj, obj_in=obj_in)


application = CRUDApplication(Application)
