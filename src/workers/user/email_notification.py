import logging
from requests.exceptions import ConnectionError, Timeout, RequestException
from resend.exceptions import ResendError
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
    
    except ResendError as exc:
        logger.error(f"ResendError sending confirmation email: {exc}", exc_info=True)
        raise self.retry(exc=exc)
    except (ConnectionError, Timeout, RequestException) as exc:
        logger.error(f"Network-related error sending confirmation email: {exc}", exc_info=True)
        raise self.retry(exc=exc)
    except Exception as exc:
        logger.error(f"Unexpected error sending confirmation email: {exc}", exc_info=True)
        raise

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_reset_email_task(self, email: str, reset_url: str):
    """
    Celery task to send reset email
    """
    try:
        send_reset_email(email, reset_url)
        
    except ResendError as exc:
        logger.error(f"ResendError sending reset email: {exc}", exc_info=True)
        raise self.retry(exc=exc)
    except (ConnectionError, Timeout, RequestException) as exc:
        logger.error(f"Network-related error sending reset email: {exc}", exc_info=True)
        raise self.retry(exc=exc)
    except Exception as exc:
        logger.error(f"Unexpected error sending reset email: {exc}", exc_info=True)
        raise