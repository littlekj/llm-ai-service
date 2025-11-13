import resend
from resend.exceptions import ResendError
from pydantic import SecretStr
import logging
from src.config.settings import settings


logger = logging.getLogger(__name__)

resend_api_key = settings.RESEND_API_KEY.get_secret_value() \
    if isinstance(settings.RESEND_API_KEY, SecretStr) else settings.RESEND_API_KEY
resend_from_email = settings.RESEND_FROM_EMAIL


def send_confirmation_email(to_email: str, confirm_url: str):
    """
    发送邮箱确认邮件（用于新用户注册激活）
    """
    if not resend_api_key:
        logger.warning(f"Resend API key not configured. Skipping confirmation email to {to_email}.")
    
    resend.api_key = resend_api_key
    
    params: resend.Emails.SendParams = {
        "from": f"Quillnk <{resend_from_email}>",
        "to": [to_email],
        "subject": "Confirm your email",
        "html": f"""
            <p>Welcome! Click the link below to confirm your email address and activate your account:</p>
            <p><a href="{confirm_url}">{confirm_url}</a></p>
            <p>If you didn't create, please ignore this email.</p>
        """,
    }
    
    try:
        email = resend.Emails.send(params)
        logger.info(f"Confirmation email sent to {to_email}, id={email.get('id')}")
        return {"success": True, "id": email.get('id')}
    
    except ResendError as e:
        logger.error(f"ResendError sending confirmation email to {to_email}: {e}", exc_info=True)
        return {"success": False, "error": "resend_api_error"}
    except Exception as e:
        logger.exception(f"Unexpected error sending confirmation email to {to_email}", exc_info=True)
        return {"success": False, "error": "unexpected_error"}


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


# send_confirmation_email("quillnk@qq.com", "https://ai.quillnk.com/confirm?token=abc123")
# send_reset_email("quillnk@qq.com", "https://ai.quillnk.com/reset-password?token=abc123")
