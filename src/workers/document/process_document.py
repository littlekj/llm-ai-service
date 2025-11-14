from celery import group
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone, timedelta
from uuid import UUID
from minio.error import S3Error, ServerError, InvalidResponseError
from requests.exceptions import ConnectionError, Timeout, RequestException
# 或
# from urllib3.exceptions import ConnectionError, NewConnectionError
from sqlalchemy.exc import SQLAlchemyError

import hashlib
import uuid
import mimetypes
import logging

from src.workers.celery_app import celery_app
from src.workers.system.request_id_helper import set_request_id_from_task
from src.core.database import get_sync_db
from src.models.user import User
from src.models.document import Document
from src.models.document import StorageStatus, ProcessingStatus
from src.schemas.document import DocumentCreate
from src.crud.document import DocumentCRUD
from src.utils.minio_storage import MinioClient
from src.config.settings import settings
        

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
    # autoretry_for=(ConnectionError, Timeout),  # 自动重试网络错误
    retry_backoff=True,  # 指数退避
    soft_time_limit=120,  # 软超时
    time_limit=150,  # 硬超时                 
)
def upload_document_task(
    self, 
    user_id: UUID,
    temp_file_path: str, 
    original_filename: str, 
) -> Optional[dict]:
    """
    处理文档对象上传任务: 文件上传到对象存储，创建数据库记录
    """
    task_id = self.request.id   # Celery 任务 ID
    request_id = set_request_id_from_task(self)  # 从 Celery 任务中恢复 request_id 到当前进程的上下文
    temp_path = Path(temp_file_path)
    db_doc = None  # 初始化为 None，避免 NameError
    storage_key = None
     
    logger.info(f"[Req: {request_id}][Task: {task_id}] Starting upload | User={user_id} | File={original_filename}")
    
    # 初始化任务状态
    self.update_state(
        state="PROGRESS", 
        meta={
            "status": "starting",
            "progress": 0,        # 记录任务进度
            "exc_type": None,     # 记录异常类型
            "exc_message": None,  # 记录异常信息
        }
    )
    
    try:
        # 校验临时文件
        if not temp_path.exists():
            raise FileNotFoundError(f"Temporary file not found: {temp_path}")
        if temp_path.stat().st_size == 0:
            raise ValueError(f"Empty file not allowed: {temp_path}")
        
        # 更新进度：文件校验完成
        self.update_state(
            state="PROGRESS", 
            meta={
                "status": "validating",
                "progress": 10,
            }
        )
        
        # 计算文件哈希
        hash_sha256 = hashlib.sha256()
        file_size = 0 
        try:
            with open(temp_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_sha256.update(chunk)
                    file_size += len(chunk)
        except Exception as e:
            raise Exception(f"Failed to read temporary file {temp_path}: {e}")
        checksum = hash_sha256.hexdigest()
        
        # 更新进度：哈希计算完成
        self.update_state(
            state="PROGRESS", 
            meta={
                "status": "checking_duplicates",
                "progress": 30,
            }
        )
        
        with get_sync_db() as db_session:  # 使用上下文管理器，自动 commit/rollback/close
            # 检查是否存在重复文件
            document_crud = DocumentCRUD()
            existing_doc = document_crud.get_by_checksum_and_user(
                db=db_session,
                checksum=checksum,
                user_id=user_id
            )
            
            # 如果存在重复文件记录，检查 MinIO 中是否已有该版本
            if existing_doc:
                latest_version = minio_client._latest_version(existing_doc.storage_key)
                # 如果 MinIO 中已有该版本，直接返回现有记录
                if latest_version:
                    logger.warning(
                        f"Duplicate file detected: "
                        f"document id={existing_doc.id}, checksum={checksum} already exists in DB and MinIO"
                    )
                    
                    result = {
                        "status": "success",
                        "task_id": task_id,
                        "message": "Document already exists",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "doc_id": existing_doc.id,
                        "filename": existing_doc.filename,
                        "size_bytes": existing_doc.size_bytes,
                        "content_type": existing_doc.content_type,
                        "storage_status": existing_doc.storage_status,
                        "processing_status": existing_doc.processing_status,
                        "checksum": checksum,
                        "created_at": existing_doc.created_at.isoformat(),
                        "updated_at": existing_doc.updated_at.isoformat(),
                        "version_id": existing_doc.version_id,
                    }
                    self.update_state(
                        state="SUCCESS", 
                        meta={
                            "result": result,
                            "progress": 100,
                        }
                    )
                    return result
                    
                logger.warning(
                    f"Duplicate file detected: "
                    f"document id={existing_doc.id}, checksum={checksum} only exists in DB but not in MinIO"
                )
                logger.info(f"Proceeding to re-upload the file to MinIO")
                
                # 更新进度：开始更新文档记录
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "updating_record",
                        "progress": 40,
                    }
                )
                        
                # 生成对象存储 key
                suffix = Path(original_filename).suffix
                ext = suffix[1:].lower() if suffix else 'bin'
                upload_time = datetime.now(timezone.utc)
                storage_path = upload_time.strftime("%Y/%m/%d")
                file_id = str(uuid.uuid4().hex)
                new_storage_key = f"uploads/{storage_path}/{file_id}.{ext}"
                
                storage_key = existing_doc.storage_key if existing_doc.storage_key else new_storage_key
                
                # 获取文件类型
                content_type = mimetypes.guess_type(original_filename)[0] or 'application/octet-stream'
            
                # 写入数据库
                doc_in = DocumentCreate(
                    filename=original_filename,  # Schemas 自动处理
                    content_type=content_type,
                )
                db_doc = document_crud.update_record_for_doc(
                    db=db_session,
                    doc=existing_doc, 
                    user_id=user_id,
                    obj_in=doc_in,
                    storage_key=storage_key,
                    size_bytes=file_size,
                    checksum=checksum,
                    storage_status=StorageStatus.ACTIVE,
                    processing_status=ProcessingStatus.PROCESSING,
                )
                if not db_doc:
                    raise Exception(f"Failed to update document record in database")
                logger.info(f"Document record updated in database, doc_id={db_doc.id}")

                # 更新进度：开始上传文件
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "uploading",
                        "progress": 60,
                    }
                )
                
                # 生成文件元数据
                metadata = {
                    "x-meta-file-id": file_id,
                    "x-meta-user-id": str(user_id),
                    "x-meta-upload-time": upload_time.isoformat(),  # 必须是字符串
                    "x-meta-original-filename": original_filename,
                }
                
                # 上传到对象存储 MinIO/S3
                try:
                    result = minio_client.upload_file(
                        object_name=storage_key,
                        file_path=str(temp_path),
                        content_type=content_type,
                        metadata=metadata,
                    )
                    logger.info(
                        f"Uploaded to MinIO | ETag={result.etag} | "
                        f"VersionID={result.version_id}"
                    )
                except (ConnectionError, Timeout, RequestException) as net_exc:
                    logger.warning(f"Network error while uploading file to MinIO: {net_exc}")
                    raise
                except S3Error as s3e:
                    if s3e.code == "AccessDenied":
                        logger.error(f"MinIO access denied: {s3e.message}")
                        raise RuntimeError("MinIO permission error") from s3e
                    else:
                        logger.error(f"MinIO S3Error: {s3e.code} - {s3e.message}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected MinIO upload error: {e}", exc_info=True)
                    raise
                
                # 更新进度：更新数据库状态
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "updating_status",
                        "progress": 80,
                    }
                )
            
                # 更新数据库状态为 ARCHIVED
                doc_in_db = document_crud.get_by_id(db_session, db_doc.id, user_id)
                if not doc_in_db:
                    raise RuntimeError("Document not found after upload")
                document_crud.update_status(
                    db=db_session,
                    doc=db_doc,
                    storage_status=StorageStatus.ARCHIVED,
                    processing_status=ProcessingStatus.SUCCESS,
                    version_id=result.version_id
                )
                logger.info(f"Document status marked as ARCHIVED in DB")

                # 更新进度：任务完成
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "finalizing",
                        "progress": 90,
                    }
                )

                # 构造返回结果
                result_data = {
                    "status": "success",
                    "task_id": task_id,
                    "doc_id": db_doc.id,
                    "filename": db_doc.filename,
                    "size_bytes": db_doc.size_bytes,
                    "content_type": db_doc.content_type,
                    "storage_status": db_doc.storage_status,
                    "processing_status": db_doc.processing_status,
                    "created_at": db_doc.created_at.isoformat(),
                    "updated_at": db_doc.updated_at.isoformat(),
                    "version_id": result.version_id,
                    "checksum": checksum,
                }
                
                # 更新任务状态
                self.update_state(
                    state="SUCCESS",
                    meta={
                        "result": result_data,
                        "progress": 100,
                    }
                )
                logger.info(f"Upload task completed successfully | doc_id={db_doc.id}")
                
                return result_data
            
            else:
                # 更新进度：开始创建文档记录
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "creating_record",
                        "progress": 40,
                    }
                )
                        
                # 生成对象存储 key
                suffix = Path(original_filename).suffix
                ext = suffix[1:].lower() if suffix else 'bin'
                upload_time = datetime.now(timezone.utc)
                storage_path = upload_time.strftime("%Y/%m/%d")
                file_id = str(uuid.uuid4().hex)
                storage_key = f"uploads/{storage_path}/{file_id}.{ext}"
                
                # 获取文件类型
                content_type = mimetypes.guess_type(original_filename)[0] or 'application/octet-stream'
            
                # 写入数据库
                doc_in = DocumentCreate(
                    filename=original_filename,  # Schemas 自动处理
                    content_type=content_type,
                )
                db_doc = document_crud.create_record_with_user_id(
                    db=db_session, 
                    user_id=user_id,
                    obj_in=doc_in,
                    storage_key=storage_key,
                    size_bytes=file_size,
                    checksum=checksum,
                    storage_status=StorageStatus.ACTIVE,
                    processing_status=ProcessingStatus.PROCESSING,
                )
                if not db_doc:
                    raise Exception(f"Failed to create document record in database")
                logger.info(f"Document record created in database, doc_id={db_doc.id}")
                
                # 更新进度：开始上传文件
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "uploading",
                        "progress": 60,
                    }
                )
                
                # 生成文件元数据
                metadata = {
                    "x-meta-file-id": file_id,
                    "x-meta-user-id": str(user_id),
                    "x-meta-upload-time": upload_time.isoformat(),  # 必须是字符串
                    "x-meta-original-filename": original_filename,
                }
                
                # 上传到对象存储 MinIO/S3
                try:
                    result = minio_client.upload_file(
                        object_name=storage_key,
                        file_path=str(temp_path),
                        content_type=content_type,
                        metadata=metadata,
                    )
                    logger.info(
                        f"Uploaded to MinIO | ETag={result.etag} | "
                        f"VersionID={result.version_id}"
                    )
                except (ConnectionError, Timeout, RequestException) as net_exc:
                    logger.warning(f"Network error while uploading file to MinIO: {net_exc}")
                    raise
                except S3Error as s3e:
                    if s3e.code == "AccessDenied":
                        logger.error(f"MinIO access denied: {s3e.message}")
                        raise RuntimeError("MinIO permission error") from s3e
                    else:
                        logger.error(f"MinIO S3Error: {s3e.code} - {s3e.message}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected MinIO upload error: {e}", exc_info=True)
                    raise
                
                # 更新进度：更新数据库状态
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "updating_status",
                        "progress": 80,
                    }
                )
            
                # 更新数据库状态为 ARCHIVED
                doc_in_db = document_crud.get_by_id(db_session, db_doc.id, user_id)
                if not doc_in_db:
                    raise RuntimeError("Document not found after upload")
                document_crud.update_status(
                    db=db_session,
                    doc=db_doc,
                    storage_status=StorageStatus.ARCHIVED,
                    processing_status=ProcessingStatus.SUCCESS,
                    version_id=result.version_id
                )
                logger.info(f"Document status marked as ARCHIVED in DB")

                # 更新进度：任务完成
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "finalizing",
                        "progress": 90,
                    }
                )

                # 构造返回结果
                result_data = {
                    "status": "success",
                    "task_id": task_id,
                    "message": "Document uploaded successfully",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "doc_id": db_doc.id,
                    "filename": db_doc.filename,
                    "size_bytes": db_doc.size_bytes,
                    "content_type": db_doc.content_type,
                    "storage_status": db_doc.storage_status,
                    "processing_status": db_doc.processing_status,
                    "checksum": checksum,
                    "version_id": result.version_id,
                    "created_at": db_doc.created_at.isoformat(),
                    "updated_at": db_doc.updated_at.isoformat(),
                }
                
                # 更新任务状态
                self.update_state(
                    state="SUCCESS",
                    meta={
                        "result": result_data,
                        "progress": 100,
                    }
                )
                logger.info(f"Upload task completed successfully | doc_id={db_doc.id}")
                
                return result_data
        
    # 可重试异常：网络、连接、超时
    except (ConnectionError, TimeoutError, RequestException) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}] Network error: {retry_exc}")
        if self.request.retries < self.max_retries:
            self.update_state(
                state="RETRY",
                meta={
                    "status": "retrying",
                    "progress": 0,
                    "exc_type": type(retry_exc).__name__,
                    "exc_message": str(retry_exc),
                }
            )
            raise self.retry()
        else:
            # 如果重试次数用尽，则记录错误并标记任务失败
            self.update_state(
                state="FAILURE",
                meta={
                    "status": "failed",
                    "progress": 0,
                    "exc_type": type(retry_exc).__name__,
                    "exc_message": "Max retries exceeded",
                }
            )
            logger.error(f"Max retries exceeded: {retry_exc}")
            raise RuntimeError("Upload failed after max retries")
            
    # 非可重试异常：如数据库错误、逻辑错误
    except Exception as exc:
        # 确保异常信息完整
        exc_type = type(exc).__name__
        exc_message = str(exc)
        # 更新任务状态
        self.update_state(
            state="FAILURE",
            meta={
                "status": "failed",
                "progress": 0,
                "exc_type": exc_type,
                "exc_message": exc_message,
            }
        )
        logger.error(f"Permanent failure: {exc_type} - {exc_message}", exc_info=True)
        
        # 尝试标记为失败
        if db_doc:
            try:
                with get_sync_db() as retry_db:
                    doc = document_crud.get_by_id(retry_db, db_doc.id, user_id)
                    if doc:
                        document_crud.update_status(
                            db=retry_db,
                            doc=doc,
                            storage_status=StorageStatus.ACTIVE,
                            processing_status=ProcessingStatus.FAILURE,
                            error_message=str(exc)[:500]
                        )
                        retry_db.commit()
                        logger.info(f"Document status marked as FAILURE in DB")
            except Exception as e:
                logger.error(f"Failed to mark document as FAILURE in DB: {e}")
                
        # 无论是否更新状态，都抛出异常
        raise
    
    finally:
        # 清理临时文件
        if temp_path.exists():
            try:
                temp_path.unlink()
                logger.info(f"Cleaned up temp file: {temp_path}")
            except Exception as e:
                logger.error(f"Failed to delete temp file: {e}")
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def soft_delete_document_task(
    self,
    doc_id: UUID,
    user_id: UUID,
):
    """
    软删除文档：标记为已删除，并添加 MinIO delete marker
    """
    task_id = self.request.id
    logger.info(f"Soft deleting document {doc_id} by user {user_id}")
    
    with get_sync_db() as db_session:
        try:
            document_crud = DocumentCRUD()
            doc = document_crud.get_by_id(db_session, doc_id, user_id)
            if not doc:
                raise ValueError("Document not found or access denied")
            if doc.deleted_at:
                logger.info(f"Document {doc_id} already soft deleted")
                raise ValueError("Document already soft deleted")
            
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
            except (S3Error, ConnectionError, Timeout) as e:
                logger.error(f"MinIO delete marker failed: {e}")
                raise self.retry()

            # 更新数据库
            try:
                deleted_doc = document_crud.soft_delete(
                    db=db_session,
                    doc=doc,
                    deleted_at=deleted_at,
                    updated_at=deleted_at,
                    version_id=version_id
                )
                logger.info(f"Document {doc_id} marked as soft-deleted in DB")
            except Exception as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO delete marker: {e}")
                raise
            
            result = {
                "status": "success",
                "task_id": task_id,
                "message": "Document deleted successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "document": {
                    "doc_id": doc_id,
                    "filename": doc.filename,
                    "size_bytes": doc.size_bytes,
                    "content_type": doc.content_type,
                },
                "is_deleted": True,
                "deleted_at": deleted_at.isoformat(),
                "version_id": version_id,
            }
            # self.update_state(state='SUCCESS', meta={'result': result})
            logger.info(f"Soft delete task completed for document {doc_id}")

            return result
        
        except (ConnectionError, Timeout) as retry_exc:
            logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
            if self.request.retries < self.max_retries:
                raise self.retry()
            logger.error(f"Max retries reached for MinIO error")
            raise RuntimeError("Soft delete failed after max retries") from retry_exc
        except ValueError as ve:
            logger.warning(f"Validation error: {ve}")
            raise
        except Exception as exc:
            logger.error(f"Unexpected error during soft delete: {exc}", exc_info=True)
            raise
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def restore_document_task(self, doc_id: UUID, user_id: UUID, version_id: str):
    """
    恢复文档：从 MinIO 恢复对象，并更新数据库
    """
    task_id = self.request.id
    logger.info(f"Restoring document {doc_id} by user {user_id}")
    
    try:
        with get_sync_db() as db_session:
            document_crud = DocumentCRUD()
            doc = document_crud.get_soft_deleted_by_id(db_session, doc_id, user_id)
            if not doc:
                raise ValueError("Document not found or access denied")
            if not doc.deleted_at:
                logger.info(f"Document not deleted: {doc_id}")
                raise ValueError("Document not found in soft deleted state")
            
            try:
                restore_result = minio_client.restore_document(
                    object_name=doc.storage_key, 
                    version_id=version_id
                )
                updated_at = restore_result['updated_at']
                updated_at = parse_iso_datetime(updated_at)
                last_version = restore_result['last_version']
                logger.info(f"MinIO restore result: {restore_result}")
            except Exception as e:
                logger.error(f"Failed to restore document in MinIO: {e}")
                raise
                
            try:
                restored_doc = document_crud.restore(
                    db=db_session,
                    doc=doc,
                    deleted_at=None,
                    updated_at=updated_at,
                    version_id=last_version,
                )
                logger.info(f"Document {doc_id} restored in DB")
            except SQLAlchemyError as e:
                db_session.rollback()
                logger.error(f"Failed to update DB after MinIO restore: {e}", exc_info=True)
                raise
            
            result = {
                "status": "success",
                "task_id": task_id,
                "message": "Document restored successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "document": {
                    "doc_id": doc_id,
                    "filename": restored_doc.filename,
                    "size_bytes": restored_doc.size_bytes,
                    "content_type": restored_doc.content_type, 
                },
                "restored_at": updated_at.isoformat(),
                "version_id": last_version,
                "storage_status": restored_doc.storage_status,
            }
            # self.update_state(state='SUCCESS', meta={'result': result})
            logger.info(f"Restore task completed for document {doc_id}")
            
            return result
    
    except SQLAlchemyError as db_exc:
        logger.error(f"Database error during restore document: {db_exc}", exc_info=True)
        raise    
    except (ConnectionError, Timeout) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        # self.update_state(state='FAILURE', meta={'reason': "Max retries exceeded"})
        raise RuntimeError("Restore failed after max retries") from retry_exc    
    except ValueError as ve:
        # self.update_state(state='FAILURE', meta={'reason': str(ve)})
        logger.warning(f"Validation error: {ve}")
        raise
    except Exception as exc:
        # self.update_state(state='FAILURE', meta={'reason': str(exc)})
        logger.error(f"Unexpected error during restore: {exc}", exc_info=True)
        raise


@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def permanent_delete_document_task(self, doc_id: UUID, user_id: UUID):
    """
    永久删除文档：从 MinIO 删除对象，并更新数据库
    """
    task_id = self.request.id
    logger.info(f"Permanently deleting document {doc_id} by user {user_id}")  

    try:
        with get_sync_db() as db_session:
            document_crud = DocumentCRUD()
            db_doc = document_crud.get_record_include_soft_delete(db_session, doc_id, user_id)
            if not db_doc:
                logger.error(f"Document not found: id={doc_id}, user_id={user_id}")
                raise
            if not db_doc.storage_key:
                logger.error(f"Document has no storage key: id={db_doc.id}, user_id={user_id}")
                raise
            storage_key = db_doc.storage_key
            logger.info(f"Document found: id={db_doc.id}, storage_key={storage_key}")
            # 从 MinIO 永久删除对象
            minio_client.permanent_delete_document(object_name=storage_key)
            logger.info(f"MinIO permanent delete successful for document {db_doc.id}")
            # 从数据库永久删除记录
            document_crud.permanent_delete_by_id(db_session, db_doc.id, user_id)
            logger.info(f"Document permanent deleted successfully in DB")

            result = {
                "status": "success",
                "task_id": task_id,
                "message": "Permanent delete successful",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "doc_id": db_doc.id,
                "filename": db_doc.filename,
            }
            
            return result
        
    except (ConnectionError, Timeout) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        raise RuntimeError("Permanent delete failed after max retries") from retry_exc    
    except ValueError as ve:
        logger.warning(f"Validation error: {ve}")
        raise
    except Exception as exc:
        logger.error(f"Unexpected error during permanent delete document: {exc}")
        raise

@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def permanent_delete_from_s3_task(self, user_id: UUID, storage_key: str):
    """
    永久删除残余文档：从 MinIO 删除对象，不更新数据库
    """
    task_id = self.request.id
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

    except (ConnectionError, Timeout) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        raise RuntimeError("Permanent delete failed after max retries") from retry_exc    
    except ValueError as ve:
        logger.warning(f"Validation error: {ve}")
        raise
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
            
    except (ConnectionError, Timeout) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        raise RuntimeError("Restore failed after max retries") from retry_exc    
    except ValueError as ve:
        logger.warning(f"Validation error: {ve}")
        raise
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

    except (ConnectionError, Timeout) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}]: {retry_exc}")
        if self.request.retries < self.max_retries:
            raise self.retry()  
        raise RuntimeError("MinIO permanent delete failed after max retries") from retry_exc    
    except ValueError as ve:
        logger.warning(f"Validation error: {ve}")
        raise
    except Exception as exc:
        logger.error(f"Unexpected error during permanent delete: {exc}", exc_info=True)
        raise
    

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
                    "doc_id": doc.id,
                    "user_id": doc.user_id,
                    "storage_key": doc.storage_key,
                    "deleted_at": doc.deleted_at.isoformat() if doc.deleted_at else None,
                })
                
            logger.info(f"Found {len(result)} expired documents for permanent deletion")
            return result
        
    except Exception as exc:
        logger.error(f"Error finding expired documents for permanent deletion: {exc}")
        return []
    
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def schedule_permanent_deletion_task(self):
    """
    调度永久删除过期文档任务
    """
    task_id = self.request.id
    logger.info(f"Scheduling permanent delete for expired documents")

    try:
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

    except Exception as exc:
        logger.error(f"Error scheduling permanent delete: {exc}", exc_info=True)
        raise

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def download_document_task(self, user_id: UUID, doc_id: UUID):
    """
    从 MinIO 存储中下载文档
    """
    task_id = self.request.id
    set_request_id_from_task(self)
    logger.info(f"Downloading document from MinIO")
    
    try:
        with get_sync_db() as db:
            # 权限校验和获取文档元信息
            document_crud = DocumentCRUD()
            doc = document_crud.get_by_id(db, doc_id, user_id)
            if not doc:
                raise ValueError("Document not found or access denied")
            
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
                "content": content_bytes,
                "filename": doc.filename,
                "size_bytes": doc.size_bytes,
                "content_type": content_type,
            }
    
    except (ConnectionError, TimeoutError) as e:
        logger.error(f"Network error during document download: {e}")
        raise self.retry(exc=e)
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
