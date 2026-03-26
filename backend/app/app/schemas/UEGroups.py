from typing import Optional, TypeAlias, Annotated
from pydantic import BaseModel, Field, ConfigDict, constr, validator

Supi: TypeAlias = Annotated[
    str,
    Field(
        regex=r"^[0-9]{15,16}$",
        description="""String identifying a Supi that shall contain either an IMSI, a network specific identifier, a Global Cable Identifier (GCI) or a Global Line Identifier (GLI) as specified in clause 2.2A of 3GPP TS 23.003.                                                                                                                                                                             In the current version (v1.1.0) only IMSI is supported"""
    )
]


class IMSIGroupBase(BaseModel):
    UEs: list[Supi] = Field(default_factory=list)


class IMSIGroup(IMSIGroupBase):
    id: int

    class Config:
        orm_mode = True


class IMSIGroupCreate(IMSIGroupBase):
    pass


class IMSIGroupUpdate(IMSIGroupBase):
    pass


class ExternalGroupBase(BaseModel):
    exterGroupId: str = Field("Group1@domain.com")
    imsiGroupId: int


class ExternalGroup(ExternalGroupBase):

    class Config:
        orm_mode = True


class ExternalGroupCreate(ExternalGroupBase):
    pass


class ExternalGroupUpdate(ExternalGroupBase):
    pass



