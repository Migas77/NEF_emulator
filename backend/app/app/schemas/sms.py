from pydantic import BaseModel, Field, root_validator

from .analyticsExposure import Gpsi, Supi


class SMSSendRequest(BaseModel):
    gpsi: Gpsi | None = Field(default=None)
    supi: Supi | None = Field(default=None)
    text: str = Field(
        description="Text content of the SMS message",
        example="Your verification code is 123456",
    )

    @root_validator(skip_on_failure=True)
    def at_least_one_identifier_present(cls, v):
        if v.get("gpsi") is None and v.get("supi") is None:
            raise ValueError('At least one of "gpsi" or "supi" must be provided.')
        return v


class SMSSendResponse(BaseModel):
    msisdn: str = Field(description="MSISDN the message was sent to")