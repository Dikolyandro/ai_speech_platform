from __future__ import annotations

import logging
import requests

from app.core.config import settings

logger = logging.getLogger(__name__)


class EmailService:
    def _is_configured(self) -> bool:
        return bool(settings.RESEND_API_KEY and settings.SMTP_FROM_EMAIL)

    def send_verification_code(self, *, to_email: str, code: str) -> bool:
        if not self._is_configured():
            logger.warning("Email verification code for %s: %s", to_email, code)
            return False

        try:
            response = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {settings.RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": f"{settings.SMTP_FROM_NAME} <{settings.SMTP_FROM_EMAIL}>",
                    "to": [to_email],
                    "subject": "Your email verification code",
                    "text": (
                        f"Your verification code is {code}.\n\n"
                        "This code expires in 10 minutes. "
                        "If you did not create an account, you can ignore this email."
                    ),
                },
                timeout=10,
            )
            response.raise_for_status()
            return True

        except Exception:
            logger.exception("Failed to send email verification code to %s", to_email)
            return False


email_service = EmailService()