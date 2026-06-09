from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from email.utils import formataddr

from app.core.config import settings

logger = logging.getLogger(__name__)


class EmailService:
    def _is_configured(self) -> bool:
        return bool(settings.SMTP_HOST and settings.SMTP_FROM_EMAIL)

    def send_verification_code(self, *, to_email: str, code: str) -> bool:
        if not self._is_configured():
            logger.warning(
                "Email verification code for %s: %s",
                to_email,
                code,
            )
            return False

        message = EmailMessage()
        message["Subject"] = "Your email verification code"
        message["From"] = formataddr(
            (settings.SMTP_FROM_NAME, settings.SMTP_FROM_EMAIL or "")
        )
        message["To"] = to_email
        message.set_content(
            f"Your verification code is {code}.\n\n"
            "This code expires in 10 minutes. "
            "If you did not create an account, you can ignore this email."
        )

        try:
            if getattr(settings, "SMTP_USE_SSL", False):
                smtp_client = smtplib.SMTP_SSL(
                    settings.SMTP_HOST,
                    settings.SMTP_PORT,
                    timeout=10,
                )
            else:
                smtp_client = smtplib.SMTP(
                    settings.SMTP_HOST,
                    settings.SMTP_PORT,
                    timeout=10,
                )

            with smtp_client as smtp:
                if settings.SMTP_USE_TLS:
                    smtp.starttls()

                if (
                    settings.SMTP_USERNAME
                    and settings.SMTP_PASSWORD
                ):
                    smtp.login(
                        settings.SMTP_USERNAME,
                        settings.SMTP_PASSWORD,
                    )

                smtp.send_message(message)

        except Exception:
            logger.exception(
                "Failed to send email verification code to %s",
                to_email,
            )
            return False

        return True


email_service = EmailService()