from typing import Optional
from pydantic import BaseModel, IPvAnyAddress, constr

_MacAddress = constr(regex=r"^([0-9a-fA-F]{2})((-[0-9a-fA-F]{2}){5})$")


class ApplicationBase(BaseModel):
    app_id: str
    ip_address_v4: IPvAnyAddress
    mac_address: _MacAddress
    description: Optional[str] = None


class ApplicationCreate(ApplicationBase):
    pass


class ApplicationUpdate(BaseModel):
    ip_address_v4: IPvAnyAddress
    mac_address: _MacAddress
    description: Optional[str] = None


class ApplicationInDBBase(ApplicationBase):
    owner_id: Optional[int]

    class Config:
        orm_mode = True


class Application(ApplicationInDBBase):
    id: Optional[int]


class ApplicationInDB(ApplicationInDBBase):
    pass

