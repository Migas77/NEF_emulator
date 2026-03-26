from typing import TYPE_CHECKING
from app.db.base_class import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Table
from sqlalchemy.orm import relationship


if TYPE_CHECKING:
    from .user import User  # noqa: F401
    from .gNB import gNB  # noqa: F401
    from .Cell import Cell  # noqa: F401
    from .UE import UE # noqa: F401


# exterGroupId represents an external group identifier and identifies a group of UEs, as defined bellow:
# An External Group Identifier maps to an IMSI-Group Identifier
# An IMSI-Group Identifier maps to zero, one or several External Group Identifiers
# IMSI-Group contains a group of IMSIs which can be resolved to MSISDN or an External Identifier


IMSIGroup_UE_membership = Table(
    "imsigroup_ue_membership",
    Base.metadata,
    Column("ue_id", Integer, ForeignKey("ue.id"), primary_key=True),
    Column("imsiGroupId", Integer, ForeignKey("imsigroup.id"), primary_key=True),
)


class IMSIGroup(Base):
    # id for db/primary key
    id = Column(Integer, primary_key=True, index=True)

    # Relationships
    external_group_identifiers = relationship("ExternalGroup", back_populates="imsi_group")
    UEs = relationship("UE", secondary="imsigroup_ue_membership", back_populates="imsi_groups")


class ExternalGroup(Base):
    # id for db/primary key
    id = Column(Integer, primary_key=True, index=True)

    # e.g. Group1@domain.com
    exterGroupId = Column(String, unique=True, index=True)

    # Foreign Keys
    imsiGroupId = Column(Integer, ForeignKey("imsigroup.id"), nullable=True, default=None)

    # Relationships
    imsi_group = relationship("IMSIGroup", back_populates="external_group_identifiers")

