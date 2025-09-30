from celery import Celery

from src.config.settings import settings



celery_app = Celery(
    "llm_service_worker",
    broker=settings.CELERY_BROKER_URL,  # 指定消息代理中间件
    backend=settings.CELERY_RESULT_BACKEND,  # 指定结果存储后端
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,
    broker_heartbeat=30,
    broker_connection_timeout=30,
    broker_pool_limit=10,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    task_acks_late=True,    
)


# 自动发现任务（推荐）
celery_app.autodiscover_tasks(["src.tasks.document.process_document"])

__all__ = ["celery_app"]