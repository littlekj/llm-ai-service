import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from uuid import UUID
from urllib3.exceptions import ConnectionError, NewConnectionError

from src.workers.celery_app import celery_app
from src.workers.system.request_id_helper import set_request_id_from_task
from src.core.database import get_sync_db
from src.crud.document_job import DocumentJobCRUD
from src.utils.minio_storage import MinioClient
from src.config.settings import settings
from src.services.vector_service import VectorizationService
from src.core.exceptions import (
    ResourceConflictError,
    BusinessLogicError,
    DatabaseError,
    ExternalServiceError,
)


logger = logging.getLogger(__name__)

# 初始化Minio客户端
minio_client = MinioClient(
    endpoint_url=settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
    bucket_name=settings.MINIO_BUCKET_NAME
)

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_document_task(self, previous_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理文档向量化任务
    :param previous_result: 上一个任务的结果
    :return: 处理结果
    """
    task_id = self.request.id
    chain_id = self.request.root_id
    request_id = set_request_id_from_task(self)
    
    # 从上游任务获取上下文信息
    doc_id = previous_result["document"].get("id")
    user_id = previous_result["document"].get("user_id")
    parent_job_id = previous_result["document_job"].get("id")
    parent_stage_order = previous_result["document_job"].get("stage_order", 0)

    job_crud = DocumentJobCRUD()
    logger.info(f"Starting document vectorization task for doc: {doc_id}, via task: {task_id}")
    
    try:
        with get_sync_db() as db_session:
            vectorization_service = VectorizationService()
            result = vectorization_service.process_document_pipeline(
                db=db_session,
                doc_id=UUID(doc_id), 
                user_id=UUID(user_id),
                task_id=task_id,
                parent_job_id=UUID(parent_job_id) if parent_job_id else None,
                chain_id=chain_id,
                trace_id=request_id,
                stage_order=parent_stage_order,
            )
            
        logger.info(f"Document vectorization task finished for doc: {doc_id}, via task: {task_id}")
        
        return {
            "status": "success",
            "task_id": task_id,
            "message": "Document vectorization completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "document": {
                "id": str(doc_id),
                "user_id": str(user_id),
            },
            **result
        }
    
    except (BusinessLogicError, ResourceConflictError, DatabaseError) as e:
        # 业务错误，不需要重试
        logger.error(f"Failed to process document: {doc_id}, via task: {task_id}. error: {str(e)}", exc_info=True)
        raise
    except (ExternalServiceError) as retry_exc:
        # 处理 Celery 相关的重试逻辑
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}] Network error: {retry_exc}")
        raise self.retry(exc=retry_exc)
        # retry_job = retry_exc.details.get("retry_job_id")
        # cause = retry_exc.details.get("cause")
        # with get_sync_db() as db_session:
        #     retry_job = job_crud.get_document_job_by_id(db_session, retry_job)

        #     if retry_job and retry_job.is_retryable():
        #         raise self.retry(
        #             exc=cause,
        #             countdown=retry_job.retry_delay,
        #             max_retries=retry_job.max_retries,
        #         )
        #     else:
        #         raise cause
    except Exception as e:
        logger.error(f"Failed to process document: {doc_id}, via task: {task_id}. error: {str(e)}", exc_info=True)
        raise
