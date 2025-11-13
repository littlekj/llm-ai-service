from celery import shared_task
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, and_, or_
import logging

from src.workers.celery_app import celery_app
from src.core.database import get_sync_db
from src.models.user import User

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_unconfirmed_users(self):
    """
    清理注册时间超过7天的未验证邮箱的用户
    """
    cutoff = datetime.now() - timedelta(days=7)
    stats = {"total_cleaned": 0, "errors": []}

    try:
        with get_sync_db() as db:
            try:
                # 使用批量更新提供性能
                stmt = select(User).where(
                    User.email_confirmed_at.is_(None),
                    User.created_at < cutoff,
                    User.is_active.is_(False),
                    User.is_deleted.is_(False)
                )
                
                result = db.execute(stmt)
                users = result.scalars().all()
                
                if not users:
                    logger.info("No unconfirmed users to cleanup")
                    return stats
                
                # 批量更新
                for user in users:
                    try:
                        user.is_deleted = True
                        user.deleted_at = datetime.now(timezone.utc)
                        stats["total_cleaned"] += 1
                    except Exception as e:
                        error_msg= f"Failed to mark user {user.id} as deleted: {str(e)}"
                        stats["errors"].append(error_msg)
                        logger.error(error_msg)
                    
                db.commit()
                logger.info(f"Successfully cleaned up {stats['total_cleaned']} unconfirmed user(s)")
                
                return stats
            
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during cleanup: {str(e)}")
                raise self.retry(exc=e)
    except Exception as e:
        logger.error(f"Cleanup unconfirmed users task failed: {str(e)}")
        raise self.retry(exc=e)
    

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_deleted_users(self):
    """
    清理删除时间超过30天的用户
    """
    cutoff = datetime.now() - timedelta(days=30)    
    stats = {"total_cleaned": 0, "errors": []}

    try:
        with get_sync_db() as db:
            try:
                # 使用批量更新提供性能
                stmt = select(User).where(
                    User.is_deleted.is_(True),
                    User.deleted_at < cutoff
                )
                
                result = db.execute(stmt)
                users = result.scalars().all()

                if not users:
                    logger.info("No deleted users to cleanup")
                    return stats

                # 批量更新
                for user in users:
                    try:
                        db.delete(user)
                        stats["total_cleaned"] += 1
                    
                    except Exception as e:
                        error_msg = f"Failed to delete user {user.id}: {str(e)}"
                        stats["errors"].append(error_msg)
                        logger.error(error_msg)

                db.commit()
                logger.info(f"Successfully cleaned up {stats['total_cleaned']} deleted user(s)")

                return stats

            except Exception as e:
                db.rollback()
                logger.error(f"Database error during cleanup: {str(e)}")
                raise self.retry(exc=e)
            
    except Exception as e:
        logger.error(f"Cleanup deleted users task failed: {str(e)}")
        raise self.retry(exc=e)