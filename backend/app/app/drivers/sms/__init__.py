from typing import Annotated

from fastapi import Depends

from app.core.config import SMSBackend, settings
from app.interfaces.sms import SMSInterface

if settings.sms.backend == SMSBackend.SMSC:
    from .smsc import SMSCSMSInterface

    _sms_interface: SMSInterface = SMSCSMSInterface(
        settings.sms.smsc_url, settings.sms.sender_id
    )
else:
    from .mock import MockSMSInterface

    _sms_interface: SMSInterface = MockSMSInterface()


async def get_sms_interface() -> SMSInterface:
    return _sms_interface


SMSInterfaceDep = Annotated[SMSInterface, Depends(get_sms_interface)]