from celery import Celery
from celery.schedules import crontab
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone, timedelta
from uuid import UUID
import hashlib
import uuid
import logging

from src.tasks.celery_app import celery_app
from src.core.database import get_sync_db
from src.models.user import User
from src.models.document import DocumentStatus
from src.schemas.document import DocumentCreate
from src.crud.document import document_crud
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

def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=2, minute=0),  # 每天凌晨2点执行
        cleanup_trash.s()
    )

def parse_iso_datetime(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        return datetime.min

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def upload_document_task(
    self, 
    user_id: int,
    temp_file_path: str, 
    original_filename: str, 
    content_type: str
) -> Optional[dict]:
    """
    处理文档对象上传任务: 文件上传到对象存储，创建数据库记录
    """
    temp_path = Path(temp_file_path)
    db_session = None
    db_doc = None  # 初始化为 None，避免 NameError
    storage_key = None
    
    # 初始化任务状态
    self.update_state(state="PROGRESS", meta={"status": "starting"})
    
    try:
        # 校验临时文件
        if not temp_path.exists():
            raise FileNotFoundError(f"Temporary file not found: {temp_path}")
        if temp_path.stat().st_size == 0:
            raise ValueError(f"Empty file not allowed: {temp_path}")
        
        # 计算文件哈希值和大小
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
        
        # 生成对象存储路径
        suffix = Path(original_filename).suffix
        ext = suffix[1:].lower() if suffix else 'bin'
        upload_time = datetime.now(timezone.utc)
        upload_data_path = upload_time.strftime("%Y/%m/%d")
        file_id = str(uuid.uuid4().hex)
        storage_key = f"uploads/{upload_data_path}/{file_id}.{ext}"
        
        # 生成文件元数据
        metadata = {
            "file_id": file_id,
            "user_id": str(user_id),
            "upload_time": upload_time,
        }
        
        # 将文件上传到对象存储 MinIO/S3
        try:
            success = minio_client.upload_fileobj(
                bucket_name=settings.MINIO_BUCKET_NAME,
                object_name=storage_key,
                file_path=str(temp_path),
                content_type=content_type,
                metadata=metadata,
                auto_create_bucket=True
            )
            if not success:
                raise Exception("MinIO upload failed: unknown reason")
            logger.info(f"File uploaded to MinIO: bucket={settings.MINIO_BUCKET_NAME}, key={storage_key}")
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Network error while uploading file to MinIO: {e}")
            raise  # 触发重试机制
        except Exception as e:
            logger.error(f"File upload failed to MinIO: {e}", exc_info=True)
            raise  # 触发重试机制
    
        # 获取数据库会话
        with get_sync_db() as db_session:  # 使用上下文管理器，自动 commit/rollback/close
            # 创建数据库记录
            try:
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
                    status= DocumentStatus.INDEXED,
                    checksum=checksum,
                )
                if not db_doc:
                    raise Exception(f"Failed to create document record in database")
                db_session.commit()
                logger.info(f"Document record created in database: id={db_doc.id}, user_id={user_id}")
            except Exception as e:
                db_session.rollback()
                logger.error(f"Failed to create document record in database: {e}", exc_info=True)
                raise
        
            # 清理临时文件
            try:
                if temp_path.exists():
                    temp_path.unlink()
                    logger.info(f"Temporary file deleted: {temp_path}")
            except Exception as e:
                logger.error(f"Failed to delete temporary file {temp_path}: {e}")
        
            file_detail = {
                "id": str(db_doc.id),
                "filename": db_doc.filename,
                "size_bytes": db_doc.size_bytes,
                "content_type": db_doc.content_type,
                "status": db_doc.status,
                "created_at": db_doc.created_at.isoformat(),
                "updated_at": db_doc.updated_at.isoformat(),
            }
            self.update_state(state="SUCCESS", meta={"result": file_detail})
            
            return file_detail
        
    # 可重试异常：网络、连接、超时
    except (ConnectionError, TimeoutError) as retry_exc:
        logger.warning(f"[Retry {self.request.retries + 1}/{self.max_retries}] Network error: {retry_exc}")
        if self.request.retries < self.max_retries and temp_path.exists():
            raise self.retry(exc=retry_exc)
        else:
            # 文件已删除，不再重试
            logger.error(f"Max retries exceeds or temp file missing, cannot retry")
            self.update_state(state="FAILURE", meta={"exc": "Max retries exceeds or temp file missing"})

    # 非可重试异常：如数据库错误、逻辑错误
    except Exception as exc:
        logger.error(f"Document processing failed permanently: {exc}", exc_info=True)
        self.update_state(state="FAILURE", meta={"exc": str(exc)})

        # 尝试更新文档状态为失败
        if db_doc:
            try:
                with get_sync_db() as retry_db:
                    doc_in_db = document_crud.get_by_id(retry_db, db_doc.id, user_id)
                    if doc_in_db:
                        document_crud.update_status(
                            db=retry_db,
                            doc=db_doc,
                            status=DocumentStatus.FAILED,
                            error_message=str(exc)[:500]
                        )
                        retry_db.commit()
                        logger.info(f"Document {db_doc.id} status marked as FAILED")
            except Exception as e:
                logger.error(f"Failed to update document status to FAILED: {e}", exc_info=True)
                
        # 无论是否更新状态，都抛出异常
        raise
    
    finally:
        # # 关闭数据库会话
        # if db_session:
        #     try:
        #         db_session.close()
        #     except Exception as e:
        #         logger.warning(f"Error closing database session: {e}")
        # 清理临时文件（双重保障）
        if temp_path.exists():
            try:
                temp_path.unlink()
                logger.info(f"Temporary file cleaned up in finally block: {temp_path}")
            except Exception as e:
                logger.error(f"Error to clean up temporary file in finally: {temp_path}, error: {e}")

@celery_app.task(bind=True, max_retries=3, default_retry_delay=10)
def clean_document_task(self, doc_id: UUID, storage_key: str):
    
    with get_sync_db() as db:
        pass
        
        
def finalize_document_task(self, doc_id: UUID, storage_key: str):
    
    pass

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_trash(self):
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    deleted_count = 0
    error_count = 0
    
    try:
        # 使用 minio-py 的 list_objects
        objects = minio_client.list_objects_in_trash("trash/")
        for obj in objects:
            trash_key = obj.object_name
            if trash_key is None:
                    logger.warning(f"Found object with None key in trash, skipping")
                    error_count += 1
                    continue
            try:
                tags = minio_client.get_object_tagging(trash_key)
                if not tags or tags.get('soft_deleted') != 'true':
                    continue
                deleted_at_str = tags.get('deleted_at')
                if not deleted_at_str:
                    continue
                
                deleted_at = parse_iso_datetime(deleted_at_str)
                if deleted_at.tzinfo is None:
                    deleted_at = deleted_at.replace(tzinfo=timezone.utc)
                    
                if deleted_at < cutoff:
                    minio_client.permanent_delete(trash_key)
                    deleted_count += 1
                    
                    with get_sync_db() as db:
                        try:
                            doc = db.query(Document).filter(Document.trash_s3_key == trash_key).first()
                            if doc:
                                doc.cleanup_status = "cleaned"
                            db.commit()
                        except Exception as e:
                            db.rollback()
                            logger.error(f"DB update failed: {str(e)}")
                        finally:
                            db.close()

            except Exception as e:
                error_count += 1
                logger.error(f"Cleanup failed for {trash_key}: {str(e)}")

        logger.info(f"Cleanup completed. Deleted: {deleted_count}, Errors: {error_count}")
        return {"deleted": deleted_count, "errors": error_count}

    except Exception as exc:
        logger.error("Cleanup task failed", exc_info=True)
        raise self.retry(exc=exc)
        