import logging

from app.interfaces.sms import SMSInterface

logger = logging.getLogger(__name__)


class MockSMSInterface(SMSInterface):
    async def send_sms(self, msisdn: str, text: str) -> None:
        logger.info("Would send SMS to %s: %s", msisdn, text)
