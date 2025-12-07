from celery import shared_task
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, update, delete, and_, or_
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
import logging

from src.workers.celery_app import celery_app
from src.core.database import get_sync_db
from src.models.user import User
from src.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_unconfirmed_users(self):
    """
    清理注册时间超过7天的未验证邮箱的用户
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    stats = {"total_cleaned": 0}

    with get_sync_db() as db:
        try:
            # 使用批量更新提供性能
            stmt = update(User).where(
                User.email_confirmed_at.is_(None),
                User.created_at < cutoff,
                User.is_active.is_(False),
                User.is_deleted.is_(False)
            ).values(
                is_deleted = True,
                deleted_at = datetime.now(timezone.utc)
            )
            
            result = db.execute(stmt)
            
            stats["total_cleaned"] = result.rowcount
            
            if stats["total_cleaned"] == 0:
                logger.info("No unconfirmed users to cleanup")
                return stats
            
            db.commit()
            logger.info(f"Successfully cleaned up {stats['total_cleaned']} unconfirmed user(s)")
            
            return stats
        
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error during cleanup: {str(e)}")
            return stats
        except OperationalError as e:
            db.rollback()
            logger.error(f"Operational error during cleanup: {str(e)}")
            raise self.retry(exc=e)
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during cleanup: {str(e)}")
            raise self.retry(exc=e)
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error during cleanup: {str(e)}")
            raise self.retry(exc=e)
    

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_deleted_users(self):
    """
    清理删除时间超过30天的用户
    """
    cutoff = datetime.now() - timedelta(days=30)    
    stats = {"total_cleaned": 0}

    with get_sync_db() as db:
        try:
            # 使用批量删除
            stmt = delete(User).where(
                User.is_deleted.is_(True),
                User.deleted_at < cutoff
            )
            
            result = db.execute(stmt)
            stats["total_cleaned"] = result.rowcount
            
            if stats["total_cleaned"] == 0:
                logger.info("No deleted users to cleanup")
                return stats

            db.commit()
            logger.info(f"Successfully cleaned up {stats['total_cleaned']} deleted user(s)")

            return stats

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error during cleanup: {str(e)}")
            return stats
        except SQLAlchemyError as e:
            # 连接断开等网络/数据库服务错误，进行重试
            db.rollback()
            logger.error(f"Database error during cleanup: {str(e)}")
            raise self.retry(exc=e)
        except Exception as e:
            db.rollback()
            logger.error(f"Database error during cleanup: {str(e)}")
            raise self.retry(exc=e)