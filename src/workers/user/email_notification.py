import logging
from src.workers.celery_app import celery_app
from src.utils.mailer import send_confirmation_email
from src.utils.mailer import send_reset_email


logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_confirmation_email_task(self, email: str, confirm_url: str):
    """
    Celery task to send account confirmation email
    """
    try:
        send_confirmation_email(email, confirm_url)
    except Exception as exc:
        logger.error(f"Confirmation email task failed: {exc}", exc_info=True)
        raise self.retry(exc=exc)

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_reset_email_task(self, email: str, reset_url: str):
    """
    Celery task to send reset email
    """
    try:
        send_reset_email(email, reset_url)
    except Exception as exc:
        logger.error(f"Task failed: {exc}", exc_info=True)
        raise self.retry(exc=exc)