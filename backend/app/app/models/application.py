from typing import TYPE_CHECKING
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base_class import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401


class Application(Base):
    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(String, index=True, unique=True)
    ip_address_v4 = Column(String, index=True)
    mac_address = Column(String)
    description = Column(String)

    owner_id = Column(Integer, ForeignKey("user.id"))
    owner = relationship("User", back_populates="applications")

