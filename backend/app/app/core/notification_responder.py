import logging
from typing import Any, Optional

import httpx
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

class NotificationResponder:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10, connect=3.05, read=27))

    async def send_notification(self, notificationDestination: str, json: Any, *, expected_status_code: int = 204) -> Optional[str]:
        """
        Sends a notification to the destination.

        Returns the new notification destination if a permanent redirect was encountered
        """
        next_destination = notificationDestination
        permanent_redirect = None

        if isinstance(json, BaseModel):
            json = jsonable_encoder(json.dict(exclude_unset=True))

        for _ in range(5):
            res = await self.client.post(
                next_destination,
                json=json,
            )

            if res.status_code == expected_status_code:
                logging.debug("Notification delivered successfully")
                break
                
            if res.is_success:
                logging.warning("Received unexpected 2xx status code for notification, accepting as delivered")
                break

            if res.status_code == 307 or res.status_code == 308:
                location = res.headers.get("Location")
                if location is None:
                    raise Exception("Received redirect while delivering notification but no location was given")
                next_destination = location

                if res.status_code == 308:
                    permanent_redirect = location
                continue

            raise Exception(f"Error while delivering notification (status code: {res.status_code}): {res.text}")
        else:
            raise Exception("Exceeded redirection limit while trying to deliver notification")

        return permanent_redirect

notification_responder = NotificationResponder()
