from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.param_functions import Path
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app import crud, models, schemas
from app.api import deps
from .utils import ReportLogging

router = APIRouter()
router.route_class = ReportLogging


@router.get("", response_model=List[schemas.Application])
def read_applications(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Retrieve registered applications.
    """
    if crud.user.is_superuser(current_user):
        return crud.application.get_multi(db, skip=skip, limit=limit)
    return crud.application.get_multi_by_owner(db=db, owner_id=current_user.id, skip=skip, limit=limit)


@router.post("", response_model=schemas.Application)
def create_application(
    *,
    db: Session = Depends(deps.get_db),
    item_in: schemas.ApplicationCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Register a new application.
    """
    if crud.application.get_by_app_id(db=db, app_id=item_in.app_id):
        raise HTTPException(status_code=409, detail="An application with this app_id already exists")
    try:
        return crud.application.create_with_owner(db=db, obj_in=item_in, owner_id=current_user.id)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Could not register application with the provided details")


@router.get("/{app_id}", response_model=schemas.Application)
def read_application(
    *,
    db: Session = Depends(deps.get_db),
    app_id: str = Path(..., description="The app_id of the application to retrieve"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Get application by app_id.
    """
    app = crud.application.get_by_app_id(db=db, app_id=app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not crud.user.is_superuser(current_user) and app.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Application not found")
    return app


@router.put("/{app_id}", response_model=schemas.Application)
def update_application(
    *,
    db: Session = Depends(deps.get_db),
    app_id: str = Path(..., description="The app_id of the application to update"),
    item_in: schemas.ApplicationUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Update a registered application.
    """
    app = crud.application.get_by_app_id(db=db, app_id=app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not crud.user.is_superuser(current_user) and app.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Application not found")

    try:
        return crud.application.update(db=db, db_obj=app, obj_in=item_in)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="An application with this app_id already exists")


@router.delete("/{app_id}", response_model=schemas.Application)
def delete_application(
    *,
    db: Session = Depends(deps.get_db),
    app_id: str = Path(..., description="The app_id of the application to delete"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Delete a registered application.
    """
    app = crud.application.get_by_app_id(db=db, app_id=app_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not crud.user.is_superuser(current_user) and app.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Application not found")
    return crud.application.remove(db=db, id=app.id)