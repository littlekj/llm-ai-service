import hashlib
import uuid
import mimetypes
import logging
import asyncio
from celery import group
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone, timedelta
from uuid import UUID
from minio.error import S3Error, ServerError, InvalidResponseError
from requests.exceptions import ConnectionError, Timeout, RequestException
# from urllib3.exceptions import ConnectionError, NewConnectionError
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from src.workers.celery_app import celery_app
from src.workers.system.request_id_helper import set_request_id_from_task
from src.services.document_service import DocumentService
from src.core.database import get_sync_db
from src.models.document import Document, StorageStatus
from src.schemas.document import DocumentCreate
from src.crud.document import DocumentCRUD
from src.crud.document_job import DocumentJobCRUD
from src.utils.minio_storage import MinioClient
from src.config.settings import settings
from src.core.exceptions import (
    ResourceConflictError,
    NotFoundError,
    BusinessLogicError,
    DatabaseError,
    ExternalServiceError,
    ValidationError,
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

def parse_iso_datetime(s: str) -> datetime:
    """
    解析 ISO 8601 格式的日期时间字符串为 datetime 对象
    """
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        return datetime.min

@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    retry_backoff=True,  # 指数退避
    soft_time_limit=120,  # 软超时
    time_limit=150,  # 硬超时               
)
def upload_document_task(
    self, 
    user_id: str,
    # doc_id: Optional[str],
    filename: str,
    temp_path: str, 
) -> Optional[dict]:
    
    task_id = self.request.id   # Celery 任务 ID
    chain_id = self.request.root_id  # Celery 任务链 ID
    request_id = set_request_id_from_task(self)  # 从 Celery 任务中恢复 request_id 到当前进程的上下文

    context = {"task_id": task_id, "chain_id": chain_id, "trace_id": request_id, "stage_order": 1}
    
    job_crud = DocumentJobCRUD()
    
    logger.info(f"[Req: {request_id}][Task: {task_id}] Starting upload | User={user_id} | File={filename}")
    
    try:
        with get_sync_db() as db_session:
            document_service = DocumentService()
            result = document_service.upload_document(
                db=db_session,
                user_id=UUID(user_id),
                # doc_id=UUID(doc_id) if doc_id else None,
                filename=filename,
                temp_path=Path(temp_path),
                context=context,
            )
            
            # 清理临时文件
            cleanup_temp_file(Path(temp_path))
            
            return {
                "status": "success",
                "task_id": task_id,
                "message": "Document vectorization completed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "document": {
                    "id": str(result["document"]["id"]),
                    "user_id": str(result["document"]["user_id"]),
                    "filename": result["document"]["filename"],
                    "storage_status": result["document"]["storage_status"],
                },
                "document_job": {
                    "id": str(result["document_job"]["id"]),
                    "document_id": str(result["document_job"]["document_id"]),
                    "job_type": result["document_job"]["job_type"],
                    "status": result["document_job"]["status"],
                    "stage_order": result["document_job"]["stage_order"],
                }
            }
        
    except (ResourceConflictError, BusinessLogicError, ValidationError) as e:
        # 业务错误，不需要重试
        cleanup_temp_file(Path(temp_path))
        logger.error(f"Failed to upload document: via task: {task_id}. Business error: {str(e)}", exc_info=True)
        raise
    except (ExternalServiceError, DatabaseError) as retry_exc:
        # 处理 Celery 相关的重试逻辑
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}] Network error: {retry_exc}")
        raise self.retry(exc=retry_exc)
    except Exception as e:
        cleanup_temp_file(Path(temp_path))
        logger.error(f"Failed to upload document: via task: {task_id}. Error: {str(e)}", exc_info=True)
        raise
    
def cleanup_temp_file(path: Path):
        """
        清理临时文件
        """
        try:
            if path and path.exists():
                path.unlink()
                logger.info(f"Cleaned up temp file: {path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {path}: {e}")
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def soft_delete_document_task(
    self,
    doc_id: str,
    user_id: str,
):
    """
    软删除文档：标记为已删除，并添加 MinIO delete marker
    """
    task_id = self.request.id
    doc_id = UUID(doc_id)
    user_id = UUID(user_id)
    set_request_id_from_task(self)
    
    logger.info(f"Starting soft delete task {task_id} for document {doc_id}")    
    
    with get_sync_db() as db_session:
        try:
            document_crud = DocumentCRUD()
            doc = document_crud.get_by_id(db_session, doc_id, user_id)
            if not doc:
                raise NotFoundError(resource="Document", resource_id=str(doc_id))
            
            if doc.storage_status == StorageStatus.DELETED:
                logger.warning(f"Document already soft deleted: {doc_id}")
                return {
                    "status": "success",
                    "task_id": task_id,
                    "message": "Document already soft deleted",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "document": {
                        "id": str(doc_id),
                        "filename": doc.filename,
                        "version_id": doc.version_id,
                        "is_deleted": doc.is_deleted,
                        "deleted_at": doc.deleted_at.isoformat(),
                    }
                }
            
            storage_key = doc.storage_key
            
            # 删除对象，MinIO 创建 delete marker
            try:
                result = minio_client.soft_delete_document(object_name=storage_key)
                deleted_at = result['deleted_at']
                deleted_at = parse_iso_datetime(deleted_at)
                version_id = result['delete_marker_version_id']
                logger.info(
                    f"MinIO delete marker added for {storage_key}, "
                    f"deleted_at={deleted_at}, version_id={version_id}"
                )
            except (S3Error, ConnectionError, Timeout, RequestException) as e:
                logger.error(f"MinIO delete marker failed: {e}")
                raise ExternalServiceError(
                    service_name="MinIO", 
                    message="Failed to add delete marker"
                ) from e

            # 更新数据库
            try:
                deleted_doc = document_crud.soft_delete(
                    db=db_session,
                    doc=doc,
                    deleted_at=deleted_at,
                    updated_at=deleted_at,
                    version_id=version_id
                )
                db_session.commit()
                logger.info(f"Document {doc_id} marked as soft-deleted in DB")
            except IntegrityError as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO delete marker: {e}", exc_info=True)
                raise DatabaseError(
                    message="Database integrity error during soft delete"
                ) from e
            except SQLAlchemyError as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO delete marker: {e}", exc_info=True)
                raise DatabaseError(
                    message="Database error during soft delete"
                ) from e
            
            result = {
                "status": "success",
                "task_id": task_id,
                "message": "Document deleted successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "document": {
                    "id": str(doc_id),
                    "filename": doc.filename,
                    "version_id": version_id,
                    "is_deleted": True,
                    "deleted_at": deleted_at.isoformat(),
                },
            }
            
            logger.info(f"Soft delete task completed for document {doc_id}")

            return result
        
        except ExternalServiceError as retry_exc:
            logger.error(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
            raise self.retry(exc=retry_exc)
        except DatabaseError as retry_exc:
            current_retry = self.request.retries
            max_retries = self.max_retries
            
            logger.error(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
            
            if current_retry >= max_retries:
                logger.error(f"Max retries reached for DB update. Attempting to rollback MinIO state...")
                # 达到最大重试次数，尝试回滚 MinIO 状态
                try:
                    if "version_id" in locals() and "storage_key" in locals():
                        restore_result = minio_client.restore_document(
                            object_name=doc.storage_key, 
                            version_id=version_id
                        )
                        logger.info(f"MinIO restore after failed DB update: {restore_result}")
                    else:
                        logger.warning("Cannot rollback MinIO state: missing version_id or storage_key")
                
                except Exception as rollback_exc:
                    # 如果回滚失败，记录严重错误日志并告警
                    logger.critical(
                        f"CRITICAL DATA INCONSISTENCY: Document {doc_id} is deleted in MinIO but active in DB. "
                        f"Rollback failed: {rollback_exc}"
                    )
                             
            raise self.retry(exc=retry_exc)
        
        except ValueError as ve:
            logger.warning(f"Validation error: {ve}")
            raise
        except Exception as exc:
            logger.error(f"Unexpected error during soft delete: {exc}", exc_info=True)
            raise
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def restore_document_task(self, doc_id: str, user_id: str, version_id: str):
    """
    恢复文档：从 MinIO 恢复对象，并更新数据库
    """
    task_id = self.request.id
    doc_id = UUID(doc_id)
    user_id = UUID(user_id)
    set_request_id_from_task(self)
    logger.info(f"Starting to restore document {doc_id} by user {user_id}")
    
    try:
        with get_sync_db() as db_session:
            document_crud = DocumentCRUD()
            doc = document_crud.get_soft_deleted_by_id(db_session, doc_id, user_id)
            if not doc:
                raise NotFoundError(resource="Document", resource_id=str(doc_id))
            if doc.storage_status == StorageStatus.ACTIVE:
                logger.warning(f"Document already active state: {doc_id}")
                return {
                    "status": "success",
                    "task_id": task_id,
                    "message": "Document already active",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "document": {
                        "doc_id": str(doc_id),
                        "filename": restored_doc.filename,
                        "storage_status": restored_doc.storage_status,
                        "version_id": restored_doc.version_id,
                        "restored_at": updated_at.isoformat(),
                    },
                }
            
            try:
                restore_result = minio_client.restore_document(
                    object_name=doc.storage_key, 
                    version_id=version_id
                )
                updated_at = restore_result['updated_at']
                updated_at = parse_iso_datetime(updated_at)
                last_version = restore_result['last_version']
                logger.info(f"MinIO restore result: {restore_result}")
            except (S3Error, ConnectionError, Timeout, RequestException) as e:
                logger.error(f"Failed to restore document in MinIO: {e}", exc_info=True)
                raise ExternalServiceError(
                    service_name="MinIO",
                    message="Failed to restore document"
                ) from e
                
            try:
                restored_doc = document_crud.restore(
                    db=db_session,
                    doc=doc,
                    deleted_at=None,
                    updated_at=updated_at,
                    version_id=last_version,
                )
                db_session.commit()
                
                logger.info(f"Document {doc_id} restored in DB")
            
            except IntegrityError as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO restore: {e}", exc_info=True)
                raise DatabaseError(
                    message="Database integrity error during document restore"
                ) from e
            except SQLAlchemyError as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO restore: {e}", exc_info=True)
                raise DatabaseError(
                    message="Database error during document restore"
                ) from e
            
            result = {
                "status": "success",
                "task_id": task_id,
                "message": "Document restored successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "document": {
                    "doc_id": str(doc_id),
                    "filename": restored_doc.filename,
                    "storage_status": restored_doc.storage_status,
                    "version_id": last_version,
                    "restored_at": updated_at.isoformat(),
                },
            }
            
            logger.info(f"Restore task completed for document {doc_id}")
            
            return result
    
    except ExternalServiceError as retry_exc:
        logger.error(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        raise self.retry(exc=retry_exc)
    except DatabaseError as retry_exc:
        current_retry = self.request.retries
        max_retries = self.max_retries
        logger.error(f"Database error during restore document: {retry_exc}", exc_info=True)
        
        if current_retry >= max_retries:
            try:
                if "storage_key" in locals():
                    logger.error(f"Max retries reached for DB update. Attempting to rollback MinIO state...")
                    delete_result = minio_client.soft_delete_document(
                        object_name=doc.storage_key
                    )
                    logger.info(f"MinIO soft delete after failed DB restore: {delete_result}")
                else:
                    logger.warning("Cannot rollback MinIO state: missing storage_key")
            except Exception as rollback_exc:
                logger.critical(
                    f"CRITICAL DATA INCONSISTENCY: Document {doc_id} is active in MinIO but deleted in DB. "
                    f"Rollback failed: {rollback_exc}"
                )
        raise self.retry(exc=retry_exc) 
    except ValueError as ve:
        logger.warning(f"Validation error: {ve}")
        raise
    except Exception as exc:
        logger.error(f"Unexpected error during restore: {exc}", exc_info=True)
        raise


@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def permanent_delete_document_task(self, doc_id: str, user_id: str):
    """
    永久删除文档：从 MinIO 删除对象，并更新数据库
    """
    task_id = self.request.id
    doc_id = UUID(doc_id)
    user_id = UUID(user_id)
    set_request_id_from_task(self)
    logger.info(f"Permanently deleting document {doc_id} by user {user_id}")  

    with get_sync_db() as db_session:
        document_crud = DocumentCRUD()
        db_doc = document_crud.get_record_include_soft_delete(db_session, doc_id, user_id)
        
        if not db_doc:
            logger.warning(f"Document not found: id={doc_id}, user_id={user_id}")
            return {
                "status": "success",
                "task_id": task_id,
                "message": "Document not found, considered deleted",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
        if not db_doc.storage_key:
            logger.error(f"Document has no storage key: id={db_doc.id}, user_id={user_id}")
            raise ValueError("Document storage key is missing")
        
        storage_key = db_doc.storage_key
        
        logger.info(f"Document found: id={db_doc.id}, storage_key={storage_key}")
        
        # 从 MinIO 永久删除对象
        try:
            minio_client.permanent_delete_document(object_name=storage_key)
            logger.info(f"MinIO permanent delete successful for document {db_doc.id}")
        except (S3Error, ConnectionError, Timeout, RequestException) as e:
            if isinstance(e, S3Error) and e.code == "NoSuchKey":
                logger.warning(f"Document object not found in MinIO during permanent delete: {storage_key}")
            else:
                logger.error(f"Failed to permanently delete document in MinIO: {e}", exc_info=True)
                raise self.retry(exc=ExternalServiceError(
                    service_name="MinIO", 
                    message="Failed to permanently delete document"
                ))
                        
        # 从数据库永久删除记录
        try:
            document_crud.permanent_delete_by_id(db_session, db_doc.id, user_id)
            db_session.commit()
            
            logger.info(f"Document permanent deleted successfully in DB")
            
        except IntegrityError as e:
            db_session.rollback()
            logger.critical(
                f"CRITICAL DATA INCONSISTENCY: Document {doc_id} (Key: {storage_key}) "
                f"has been permanently deleted from MinIO, but DB record deletion failed. "
                f"Manual DB cleanup required. Error: {e}"
            )
            raise ResourceConflictError(
                message="Database integrity error during permanent delete"
            ) from e
        except SQLAlchemyError as retry_exc:
            db_session.rollback()
            logger.error(f"Failed to permanently delete document in DB: {retry_exc}", exc_info=True)
            
            if self.request.retries >= self.max_retries:
                # MinIO 成功，DB 失败且重试耗尽，记录严重日志
                logger.critical(
                    f"CRITICAL DATA INCONSISTENCY: Document {doc_id} (Key: {storage_key}) "
                    f"has been permanently deleted from MinIO, but DB record deletion failed after max retries. "
                    f"Manual DB cleanup required."
                )
            raise self.retry(exc=DatabaseError(
                message="Database error during permanent delete"
            ))

        result = {
            "status": "success",
            "task_id": task_id,
            "message": "Permanent delete successful",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "document": {
                "id": str(db_doc.id),
                "filename": db_doc.filename,
                "storage_key": storage_key,
            }
        }
        
        return result

@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def permanent_delete_from_s3_task(self, user_id: str, storage_key: str):
    """
    永久删除残余文档：从 MinIO 删除对象，不更新数据库
    """
    task_id = self.request.id
    user_id = UUID(user_id)
    set_request_id_from_task(self)
    logger.info(f"Permanently deleting document {storage_key} by user {user_id}")    
    
    try:
        minio_client.permanent_delete_document(object_name=storage_key)
        logger.info(f"MinIO permanent delete successful for document {storage_key}")
        
        result = {
            "status": "success",
            "task_id": task_id,
            "message": "Permanent delete from S3 successful",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "storage_key": storage_key,
        }
            
        return result

    except (ConnectionError, Timeout, RequestException, S3Error) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        raise self.retry(exc=ExternalServiceError(
            service_name="MinIO",
            message="Failed to permanently delete document from S3"
        )) 
    except ValueError as e:
        logger.warning(f"Validation error: {e}")
        raise ValidationError(message="Invalid input") from e
    except Exception as exc:
        logger.error(f"Unexpected error during permanent delete document from s3: {exc}")
        raise
    
   
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)        
def list_objects_from_s3_task(self, prefix: Optional[str] = None, recursive: bool = True):
    """
    列出桶中所有对象（支持分页）
    """
    task_id = self.request.id
    set_request_id_from_task(self)  # 从 Celery 任务中恢复 request_id 到当前进程的上下文
    objects = []
    try:
        list_objects = minio_client.list_objects(prefix=prefix, recursive=recursive)
        for obj in list_objects:
            objects.append({
                "object_name": obj.object_name,
                "last_modified": obj.last_modified,
                "etag": obj.etag,
                'size': obj.size,
                "metadata": obj.metadata,
                "version_id": obj.version_id,
                "is_latest": obj.is_latest,
                "is_delete_marker": obj.is_delete_marker,  
            })
            
        objects.sort(key=lambda obj: obj["last_modified"], reverse=True)
        # sorted_objects = sorted(objects, key=lambda obj: obj['last_modified'], reverse=True)
            
        logger.info(f"Listed {len(objects)} objects from MinIO")
        
        result = {
            "status": "success",
            "task_id": task_id,
            "message": f"Listed {len(objects)} objects successfully",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "objects": objects,
        }
        
        return result
            
    except (ConnectionError, Timeout, RequestException, S3Error) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        raise self.retry(exc=ExternalServiceError(
            service_name="MinIO",
            message="Failed to list objects from S3"
        )) 
    except ValueError as e:
        logger.warning(f"Validation error: {e}")
        raise ValidationError(message="Invalid input") from e
    except Exception as exc:
        logger.error(f"Unexpected error listing objects: {exc}", exc_info=True)
        raise
    
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)        
def delete_document_from_s3_task(self, storage_key: str):
    """
    从 MinIO 删除文档对象
    """
    task_id = self.request.id

    try:
        minio_client.permanent_delete_document(object_name=storage_key)
        logger.info(f"MinIO permanent delete successful for document {storage_key}")
        
        result = {
            "status": "success",
            "task_id": task_id,
            "storage_key": storage_key,
        }
        
        return result

    except (ConnectionError, Timeout, RequestException, S3Error) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        raise RuntimeError("MinIO permanent delete failed after max retries") from retry_exc    
    except ValueError as e:
        logger.warning(f"Validation error: {e}")
        raise ValidationError(message="Invalid input") from e
    except Exception as exc:
        logger.error(f"Unexpected error during permanent delete: {exc}", exc_info=True)
        raise ExternalServiceError(
            service_name="MinIO",
            message="Unexpected error during permanent delete"
        ) from exc
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def schedule_permanent_deletion_task(self):
    """
    调度永久删除过期文档任务
    """
    task_id = self.request.id
    logger.info(f"Scheduling permanent delete for expired documents")

    expired_docs = _find_expired_documents()
    
    if not expired_docs:
        logger.info(f"No expired documents found for permanent deletion")
        return {
            "status": "success",
            "task_id": task_id,
            "message": "No expired documents found",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scheduled_count": 0,
            "expired_docs": [],
        }
        
    deletion_tasks = []
    for doc_info in expired_docs:
        task = permanent_delete_document_task.s(
            doc_id=doc_info["doc_id"],
            user_id=doc_info["user_id"],
            storage_key=doc_info["storage_key"],
        )
        deletion_tasks.append(task)
    
    job = group(deletion_tasks)
    result = job.apply_async()
    
    # 等待所有任务完成（可选）
    # results = result.get(disable_sync_subtasks=False)
    
    logger.info(f"Scheduled permanent delete tasks for {len(expired_docs)} documents")
    
    return {
        "status": "success",
        "task_id": task_id,
        "message": f"Scheduled {len(deletion_tasks)} permanent deletion tasks",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scheduled_count": len(deletion_tasks),
        "expired_docs": expired_docs,
    }

  
    
def _find_expired_documents() -> list[dict]:
    """
    查找已软删除且超过保留期的文档
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=settings.DOCUMENT_RETENTION_PERIOD_DAYS)
    # cutoff_date = datetime.utcnow() - timedelta(minutes=1)  # 用于测试，1 分钟
    logger.info(f"Searching for soft-deleted documents before {cutoff_date}")
    try:
        with get_sync_db() as db_session:
            document_crud = DocumentCRUD()
            expired_docs = document_crud.get_expired_soft_deleted(
                db=db_session,
                cutoff_date=cutoff_date
            )
            
            result = []
            for doc in expired_docs:
                result.append({
                    "doc_id": str(doc.id),
                    "user_id": str(doc.user_id),
                    "storage_key": doc.storage_key,
                    "deleted_at": doc.deleted_at.isoformat() if doc.deleted_at else None,
                })
                
            logger.info(f"Found {len(result)} expired documents for permanent deletion")
            return result
        
    except SQLAlchemyError as exc:
        logger.error(f"Error finding expired documents for permanent deletion: {exc}")
        raise DatabaseError(
            message="Failed to find expired documents for permanent deletion"
        ) from exc

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def download_document_task(self, user_id: str, doc_id: str):
    """
    从 MinIO 存储中下载文档（仅适用小文件）
    """
    task_id = self.request.id
    user_id = UUID(user_id)
    doc_id = UUID(doc_id)
    set_request_id_from_task(self)
    logger.info(f"Downloading document {doc_id} for user {user_id} (task_id: {task_id})")
    
    try:
        with get_sync_db() as db:
            # 权限校验和获取文档元信息
            document_crud = DocumentCRUD()
            doc = document_crud.get_by_id(db, doc_id, user_id)
            if not doc:
                raise NotFoundError("Document not found or access denied")
            
            # 从 MinIO 获取文件（返回的是 urllib3.response.HTTPResponse）
            response = minio_client.get_object(doc.storage_key)
            
            # 读取内容（流式，防止内存溢出）
            from io import BytesIO
            buffer = BytesIO()
            for chunk in response.stream(32 * 1024):
                buffer.write(chunk)
            content_bytes = buffer.getvalue()
            
            # 获取内容类型（兼容性实现）
            content_type = None
            headers = getattr(response, "headers", {})
            
            if hasattr(headers, "get"):
                content_type = headers.get("content-type") or headers.get("Content-Type")
            else:
                # fallback: 判断 getheader 对象是否可调用
                getheader = getattr(response, "getheader", None)
                if callable(getheader):
                    content_type = getheader("content-type") or getheader("Content-Type")
            
            # 如果未找到内容类型，尝试使用文件扩展名猜测
            if not content_type:
                content_type = mimetypes.guess_type(doc.storage_key)[0] or "application/octet-stream"

            # 返回文件内容和类型
            return {
                "status": "success",
                "task_id": task_id,
                "message": "Document downloaded successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "content": content_bytes,  # 仅适用于小文件，大文件请使用流式传输
                "filename": doc.filename,
                "size_bytes": doc.size_bytes,
                "content_type": content_type,
            }
    
    except (ConnectionError, TimeoutError, RequestException) as e:
        logger.error(f"Network error during document download: {e}")
        raise self.retry(exc=ExternalServiceError(
            service_name="MinIO",
            message="Network error during document download"
        ))
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied", "InvalidBucketName"]:
            logger.error(f"Non-retriable MinIO error: {e.code} - {e.message}")
            raise ExternalServiceError(
                service_name="MinIO",
                message=f"MinIO error: {e.code}"
            ) from e
        else:
            # 其他错误（如 InternalError）才重试
            logger.error(f"Retriable MinIO error: {e}", exc_info=True)
            raise self.retry(exc=ExternalServiceError(
                service_name="MinIO",
                message="MinIO temporary error"
            ))
    except Exception as e:
        logger.error(f"Unexpected error during document download: {e}", exc_info=True)
        raise 
    
    finally:
        # 关闭 HTTP 响应，释放连接
        if "response" in locals():
            try:
                if hasattr(response, "close"):
                    response.close()
                elif hasattr(response, "release_conn"):
                    response.release_conn()
            except Exception as e:
                logger.debug(f"Failed to close/release MinIO response", exc_info=True)
