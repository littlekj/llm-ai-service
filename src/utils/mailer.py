import resend
from resend.exceptions import ResendError
from pydantic import SecretStr
import logging
from src.config.settings import settings


logger = logging.getLogger(__name__)

resend_api_key = settings.RESEND_API_KEY.get_secret_value() if isinstance(
    settings.RESEND_API_KEY, SecretStr) else settings.RESEND_API_KEY
resend_from_email = settings.RESEND_FROM_EMAIL


def send_reset_email(to_email: str, reset_url: str):
    if not resend_api_key:
        logger.warning(
            f"Resend API key not configured. Skipping email to {to_email}.")
        return {"success": False, "error": "API key not configured"}
    resend.api_key = resend_api_key

    params: resend.Emails.SendParams = {
        "from": f"Quillnk <{resend_from_email}>",
        "to": [to_email],
        "subject": "Reset your password",
        "html": f"""
            <p>Click the link below to reset your password:</p>
            <p><a href="{reset_url}">{reset_url}</a></p>
            <p>If you didn't request this, please ignore this email.</p>
        """,
    }

    try:
        email = resend.Emails.send(params)
        logger.info(
            f"Password reset email sent to {to_email}, id={email.get('id')}")
        return {"success": True, "id": email.get("id")}

    except ResendError as e:
        logger.error("ResendError sending reset email to %s: %s", to_email, e)
        return {"success": False, "error": "resend_api_error"}
    except Exception as e:
        logger.exception(
            "Unexpected error sending reset email to %s", to_email)
        return {"success": False, "error": "unexpected_error"}

# send_reset_email("quillnk@qq.com", "https://ai.quillnk.com/reset-password?token=abc123")
