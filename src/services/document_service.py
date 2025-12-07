import aiofiles
import asyncio
import hashlib
import os
import tempfile
import uuid
import mimetypes
import logging
from fastapi import UploadFile
from fastapi import Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from minio.error import S3Error, ServerError, InvalidResponseError
from requests.exceptions import ConnectionError, Timeout, RequestException
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from io import BytesIO
from fastapi.responses import StreamingResponse
from fastapi import BackgroundTasks
from datetime import timezone, timedelta
from celery import chain

from src.utils.file_validator import sanitize_filename
from src.utils.file_validator import validate_file_extension
from src.utils.file_validator import validate_file_size_async
from src.schemas.document import DocumentDetailResponse
from src.models.user import User
from src.models.document import Document, StorageStatus
from src.models.document_job import DocumentJob, DocumentJobType, DocumentJobStatus
from src.crud.document import DocumentCRUD
from src.crud.document_job import DocumentJobCRUD
from src.utils.minio_storage import MinioClient
from src.middleware.request_id import request_id_ctx_var
from src.core.database import get_sync_db
from src.core.exceptions import (
    ValidationError,
    BusinessLogicError,
    NotFoundError,
    ResourceConflictError,
    ExternalServiceError,
    DatabaseError,
)


logger = logging.getLogger(__name__)

# 获取环境变量
temp_upload_dir = os.getenv("TEMP_UPLOAD_DIR")

# 转换为Path对象
if temp_upload_dir:
    TEMP_UPLOAD_DIR = Path(temp_upload_dir)
else:
    # print("tempfile.gettempdir(): ", tempfile.gettempdir())
    TEMP_UPLOAD_DIR = Path(tempfile.gettempdir()) / "llm-ai-service" / "tmp" / "uploads"
    
# 确保目录存在
TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)         

class DocumentService:
    def __init__(self):
        self.document_crud = DocumentCRUD()
        self.mino_client = MinioClient()
                    
    async def preprocessing_file(
        self, 
        upload_file: UploadFile,
    ) -> Dict[str, Any]:
        """
        统一文档处理入口
        - 上传文档到对象存储，并保存元数据到数据库
        - 文档内容提取和向量化存储
        """
        # 从上下文获取 request_id
        request_id = request_id_ctx_var.get()
        logger.info(f"Starting document upload")
        
        # 文件验证
        filename, file_ext = await self._validate_file(upload_file)

        # 生成安全文件名
        safe_filename = f"{uuid.uuid4().hex}{file_ext}"
        temp_path = TEMP_UPLOAD_DIR / safe_filename
               
        # 异步流式写入临时文件      
        await self._save_temp_file(upload_file, temp_path)
        
        return filename, temp_path
        
        
    async def _validate_file(self, upload_file: UploadFile):
        """文件验证"""
        # 检查文件是否为空
        if not upload_file or not upload_file.filename:
            raise ValidationError(message="No file provided")
        
        # 清理并获取安全文件名
        filename = sanitize_filename(upload_file.filename)
            
        # 检查文件类型
        file_ext = await validate_file_extension(filename)
        
        # 检查文件大小
        await validate_file_size_async(upload_file.file)
        
        return filename, file_ext
    
    async def _save_temp_file(self, upload_file: UploadFile, temp_path: str):
        """异步流式写入文件"""
        try:
            await asyncio.wait_for(
                self._write_file_stream(upload_file, temp_path),
                timeout=30.0  # 设置超时时间，防止文件写入时间过长
            ) 
            
            # 检查文件是否成功写入
            if not temp_path.exists():
                # 这是系统级错误，非用户请求错误
                raise ExternalServiceError(
                    service_name="File System",
                    message="Temporary file write failed",
                    details={"temp_path": str(temp_path)}
                )
            if temp_path.stat().st_size == 0:
                # 业务逻辑错误（不允许空文件）
                raise BusinessLogicError(
                    message="Uploaded file is empty",
                    details={"filename": upload_file.filename}
                )
                
            logger.info(f"Temporary file written: {temp_path}, size: {temp_path.stat().st_size}") 
          
        except asyncio.TimeoutError:
            logger.error(f"File write timeout: {upload_file.filename}")
            # 清理临时文件
            self._cleanup_temp_file(temp_path)
            raise ExternalServiceError(
                service_name="File System",
                message="File write timeout",
                details={"timeout": 30.0}
            )
        except (BusinessLogicError, ExternalServiceError):
            # 业务异常直接向上抛，抛出前清理文件
            self._cleanup_temp_file(temp_path)
            raise
        except Exception as e:
            logger.error(f"Failed to write temporary file {temp_path}: {e}", exc_info=True)
            # 清理临时文件
            self._cleanup_temp_file(temp_path)
            raise ExternalServiceError(
                service_name="File System",
                message="Unexpected error during file write",
                details={"error": str(e)}
            ) from e
        
    async def _write_file_stream(self, upload_file: UploadFile, filepath: str):
        """流式写入文件的内部协程"""
        async with aiofiles.open(filepath, "wb") as f:
            while chunk := await upload_file.read(8192):
                await f.write(chunk)
                
    def _cleanup_temp_file(self, path: Path):
        """同步清理文件（在异常处理块中，简单处理）"""
        try:
            if path.exists():
                path.unlink()
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {path}: {e}")
                
    def calc_hash_and_size(
        self,
        temp_path: Path,
    ):
        # 计算文件哈希和大小
        hash_sha256 = hashlib.sha256()
        file_size = 0 
        try:
            with open(temp_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_sha256.update(chunk)
                    file_size += len(chunk)
            checksum = hash_sha256.hexdigest()
            
            return checksum, file_size
        
        except Exception as e:
            raise Exception(f"Failed to read temporary file {temp_path}: {e}")
                
    def upload_document(
        self,
        db: Session,
        user_id: UUID,
        # doc_id: Optional[UUID],
        filename: str,
        temp_path: Path, 
        context: dict,
    ):  
        """上传文档"""
        try:
            # 检查是否存在重复文件
            checksum, file_size = self.calc_hash_and_size(temp_path)
            document_crud = DocumentCRUD()
            existing_doc = document_crud.get_by_checksum_and_user(
                db=db,
                checksum=checksum,
                user_id=user_id,
            )
            
            document_job_crud = DocumentJobCRUD()
            job_type = DocumentJobType.UPLOAD_DOCUMENT
            parent_job_id = None
            existing_job = None
            validata_doc = None
            validate_job = None
            
            if existing_doc:
                if existing_doc.storage_status == "active":
                    logger.warning(f"Document {filename} already exists and active for user {user_id}.")
                    raise ResourceConflictError(message="Document already exists and active")
                existing_job = document_job_crud.get_document_job_by_type(
                    db=db,
                    doc_id=existing_doc.id,
                    job_type=job_type,
                )
                if existing_job: 
                    logger.warning(f"Document job with type {job_type} already exists for document {existing_doc.id}")
                    if existing_job.is_terminal():
                        # 如果任务已终止，创建新任务
                        validate_job = DocumentJob(
                            document_id=existing_doc.id,
                            user_id=user_id,
                            job_type=job_type,
                            status=DocumentJobStatus.PENDING.value,
                            parent_job_id=parent_job_id,
                            retry_of_job_id=existing_job.id,
                            **context,
                        )
                        validate_job = document_job_crud.create_document_job(db, validate_job)
                        validata_doc = existing_doc
                             
                    elif existing_job.status == DocumentJobStatus.RETRYING.value:
                        # 如果已有任务且正在重试，则使用已有任务
                        validate_job = existing_job
                        validata_doc = existing_doc
                    else:
                        # 其他任务状态不允许重试
                        raise BusinessLogicError(message="Document job is not in a retryable state")  
                else:
                    validate_job = DocumentJob(
                        document_id=existing_doc.id,
                        user_id=user_id,
                        job_type=job_type,
                        status=DocumentJobStatus.PENDING.value,
                        parent_job_id=parent_job_id,
                        retry_of_job_id=existing_job.id if existing_job else None,
                        **context,
                    )
                    validate_job = document_job_crud.create_document_job(db, validate_job)
                    validata_doc = existing_doc
             
            else:
                db_doc = Document(
                    filename=filename,
                    checksum=checksum,
                    storage_status="uploading",
                    user_id=user_id,
                )
                db.add(db_doc)
                db.flush()
                db.refresh(db_doc)
                
                validate_job = DocumentJob(
                    document_id=db_doc.id,
                    user_id=user_id,
                    job_type=job_type,
                    status=DocumentJobStatus.PENDING.value,
                    parent_job_id=parent_job_id,
                    retry_of_job_id=existing_job.id if existing_job else None,
                    **context,
                )
                validate_job = document_job_crud.create_document_job(db, validate_job)
                validata_doc = db_doc
                    
            document_job_crud.mark_running(db, validate_job, job_type)
                    
            # 生成对象存储 key
            suffix = Path(filename).suffix
            ext = suffix[1:].lower() if suffix else 'bin'
            upload_time = datetime.now(timezone.utc)
            storage_path = upload_time.strftime("%Y/%m/%d")
            file_id = str(uuid.uuid4().hex)
            new_storage_key = f"uploads/{storage_path}/{file_id}.{ext}"
            
            storage_key = validata_doc.storage_key if validata_doc.storage_key else new_storage_key
            
            # 获取文件类型
            content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
            
            # 生成文件元数据
            metadata = {
                "x-meta-file-id": file_id,
                "x-meta-user-id": str(user_id),
                "x-meta-upload-time": upload_time.isoformat(),  # 必须是字符串
                "x-meta-original-filename": filename,
            }
            
            # 上传到对象存储 MinIO/S3
            try:
                result = self.mino_client.upload_file(
                    object_name=storage_key,
                    file_path=str(temp_path),
                    content_type=content_type,
                    metadata=metadata,
                )
                logger.info(
                    f"Uploaded to MinIO | ETag={result.etag} | VersionID={result.version_id}"
                )
            
            # 网络层面的错误 (连接超时、DNS解析失败等)
            except (ConnectionError, Timeout, RequestException) as net_exc:
                logger.warning(f"Network error while uploading file to MinIO: {net_exc}")
                raise ExternalServiceError(
                    service_name="MinIO/S3",
                    message="Connection to storage service failed",
                    details={"original_error": str(net_exc)},
                ) from net_exc
            # MinIO/S3 服务端错误响应
            except S3Error as s3e:
                logger.error(f"MinIO S3Error: {s3e.code} - {s3e.message}")
                if s3e.code == "AccessDenied":
                    msg = "Storage permission denied (Access Denied)"
                elif s3e.code == "NoSuchBucket":
                    msg = "Storage bucket does not exist"
                else:
                    msg = f"Storage service return error: {s3e.code}"
                    
                raise ExternalServiceError(
                    service_name="MinIO/S3",
                    message=msg,
                    details={"s3_code": s3e.code, "s3_message": s3e.message}
                ) from s3e
            except Exception as e:
                logger.error(f"Unexpected MinIO upload error: {e}", exc_info=True)
                raise 
            
            # 构建元数据
            doc_metadata = {
                "storage": {
                    "etag": result.etag,
                    "version_id": result.version_id,
                }
            }
            
            # 更新数据库    
            validata_doc = document_crud.update_record_for_doc(
                db=db,
                doc=validata_doc, 
                file_extension=ext,
                size_bytes=file_size,
                content_type=content_type,
                storage_key=storage_key,
                storage_status=StorageStatus.ACTIVE.value,
                version_id=result.version_id,
                doc_metadata=doc_metadata,
            )
            if not validata_doc:
                raise DatabaseError(message="Failed to update document record after upload")
            
            logger.info(f"Document record updated in database, doc_id={validata_doc.id}")

            # 构造返回结果
            result = {
                "document": {
                    "id": str(validata_doc.id),
                    "user_id": str(validata_doc.user_id),
                    "filename": validata_doc.filename,
                    "size_bytes": validata_doc.size_bytes,
                    "content_type": validata_doc.content_type,
                    "storage_status": validata_doc.storage_status,
                    "version_id": validata_doc.version_id,
                    "created_at": validata_doc.created_at.isoformat(),
                    "updated_at": validata_doc.updated_at.isoformat(),
                },
                "document_job": {
                    "id": str(validate_job.id),
                    "document_id": str(validate_job.document_id),
                    "job_type": validate_job.job_type,
                    "status": validate_job.status,
                    "stage_order": validate_job.stage_order,
                }
            }
            output_data = f"{job_type.value} job completed successfully"
                    
            document_job_crud.mark_success(db, validate_job, job_type, output_data)
            db.commit()
            
            logger.info(f"{job_type.value} job completed successfully for document {validata_doc.id}.")
            
            return result
        
        except (ResourceConflictError, BusinessLogicError, DatabaseError, ValidationError) as e:
            # 业务错误，不需要重试
            db.rollback()
            logger.error(f"{job_type.value} job failed: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message=str(e),
                    )
                    db.commit() # 提交状态更新
                except Exception:
                    logger.error("Failed to update job status to FAILURE")
            raise  # 重新抛出异常
        
        # 据库完整性错误
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message="Database constraint violation (Duplicate entry)",
                    )
                    db.commit()
                except Exception:
                    pass
            raise ResourceConflictError(
                message="Database integrity error during document upload",
                error_code="database_integrity_error",
            ) from e
        
        # 数据库操作错误
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database operation error: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message="Database operation error",
                    )
                    db.commit()
                except Exception:
                    pass
                
            raise DatabaseError(
                message="Database operation error during document upload"
            ) from e
        
        # 外部服务错误（可重试）
        except ExternalServiceError as e:
            db.rollback()
            logger.warning(f"{job_type.value} job failed with retryable error: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_retrying(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                    )
                    db.commit()
                except Exception:
                    pass
            raise
            
        # 未知系统错误 (不可重试，或由 Celery 默认策略处理)
        except Exception as e:
            db.rollback()
            logger.error(f"Job failed unexpectedly: {e}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message=f"System Error: {str(e)}",
                    )
                    db.commit()
                except Exception:
                    pass
            raise
        
    async def list_documents(
        self,
        db: AsyncSession,
        user: User,
        page: int,
        size: int,
    ) -> tuple[List[DocumentDetailResponse], int]:
        """
        列出用户的所有文档
        """
        try:
            items, total = await self.document_crud.get_multi_by_user_async(
                db=db, 
                user_id=user.id, 
                page=page,
                size=size
            )
            # 将 ORM 模型转换为 Pydantic 响应模型
            items = [DocumentDetailResponse.model_validate(item) for item in items]
            
            return items, total
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during listing documents for user {user.id}: {e}", exc_info=True)
            raise DatabaseError(
                message="Database error during listing documents"
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error during listing documents for user {user.id}: {e}", exc_info=True)
            raise
        
    async def list_documents_with_soft_deleted(
        self,
        db: AsyncSession,
        page: int,
        size: int,
    ) -> tuple[List[DocumentDetailResponse], int]:
        """
        列出所有软删除的文档
        """
        try:
            items, total = await self.document_crud.get_multi_with_soft_deleted_async(
                db=db,
                page=page,
                size=size,
            )
            # 将 ORM 模型转换为 Pydantic 响应模型
            items = [DocumentDetailResponse.model_validate(item) for item in items]
            
            return items, total
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during listing documents with soft deleted: {e}", exc_info=True)
            raise DatabaseError(
                message="Database error during listing documents with soft deleted",
            ) from e
        except Exception as e:
            logger.error(f"Error listing documents with soft deleted: {e}", exc_info=True)
            raise
        
    async def get_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ) -> DocumentDetailResponse:
        """根据 ID 获取文档"""
        db_doc = await self.document_crud.get_by_id_async(db, doc_id, user.id)
            
        if not db_doc:
            raise NotFoundError(resource="Document", resource_id=doc_id)

        return DocumentDetailResponse.model_validate(db_doc) 
        
    async def soft_delete_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        """软删除文档"""
        db_doc = await self.document_crud.get_by_id_async(db, doc_id, user.id)
        if not db_doc:
            raise NotFoundError(resource="Document", resource_id=doc_id)
        if db_doc.is_deleted:
            raise BusinessLogicError(message="Document already deleted")
        
        try:
            from src.workers.document.object_storage import soft_delete_document_task
            soft_delete_task = soft_delete_document_task.apply_async(
                kwargs={
                    "doc_id": str(doc_id),
                    "user_id": str(user.id),
                },
                headers={"request_id": request_id_ctx_var.get()},
            )
            
            return {
                "status": soft_delete_task.state,
                "task_id": soft_delete_task.id,
                "message": "Document soft deletion task scheduled"
            }
        
        except Exception as e:
            # 捕获消息队列连接失败等错误
            logger.error(f"Failed to schedule soft delete task for document {doc_id}: {e}", exc_info=True)
            raise ExternalServiceError(
                service_name="TaskQueue/Celery",
                message="Failed to schedule document soft delete task",
                details={"original_error": str(e)},
            ) from e
            
    async def restore_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        db_doc = await self.document_crud.get_soft_deleted_by_id_async(db, doc_id, user.id)
        if not db_doc:
            raise NotFoundError(resource="Document", resource_id=doc_id)
        if not db_doc.is_deleted:
            raise BusinessLogicError(message="Document is not soft deleted")
        if not db_doc.version_id:
            raise BusinessLogicError(message="Document has no version for restoration")
        
        print("db_doc.version_id: ", db_doc.version_id)
        
        try:
            from src.workers.document.object_storage import restore_document_task
            restore_task = restore_document_task.apply_async(
                kwargs={
                    "doc_id": str(doc_id), 
                    "user_id": str(user.id), 
                    "version_id": db_doc.version_id
                },
                headers={"request_id": request_id_ctx_var.get()},
            )
            
            return {
                "status": restore_task.state,
                "task_id": restore_task.id,
                "message": "Document restoration task scheduled"
            }

        except Exception as e:  
            logger.error(f"Failed to schedule restore task for document {doc_id}: {e}", exc_info=True)
            raise ExternalServiceError(
                service_name="TaskQueue/Celery",
                message="Failed to schedule document restore task",
                details={"original_error": str(e)},
            ) from e
        
    async def permanently_delete_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        try:
            from src.workers.document.object_storage import permanent_delete_document_task
            result = permanent_delete_document_task.apply_async(
                kwargs={
                    "doc_id": str(doc_id),
                    "user_id": str(user.id)
                },
                headers={"request_id": request_id_ctx_var.get()},
            )
            
            logger.info("Permanent delete document task submitted")
            
            return {
                "status": result.state,
                "task_id": result.id,
                "message": "Document permanent deletion task scheduled"
            }
            
        except Exception as e:
            logger.error(f"Failed to permanently delete document {doc_id}: {e}", exc_info=True)
            raise ExternalServiceError(
                service_name="TaskQueue/Celery",
                message="Failed to schedule document permanent deletion task",
                details={"original_error": str(e)},
            ) from e
        
        
    async def permanently_delete_from_s3(
        self,
        user: User,
        storage_key: str,
    ):
        try:
            from src.workers.document.object_storage import permanent_delete_from_s3_task
            result = permanent_delete_from_s3_task.apply_async(
                kwargs={
                    "user_id": str(user.id),
                    "storage_key": storage_key
                },
                headers={"request_id": request_id_ctx_var.get()},
            )
            
            return {
                "status": result.state,
                "task_id": result.id,
                "message": result.result or "Document deletion task scheduled"
            }

        except Exception as e:
            logger.error(f"Failed to permanently delete document from S3: {e}", exc_info=True)
            raise ExternalServiceError(
                service_name="TaskQueue/Celery",
                message="Failed to schedule document permanent deletion task",
                details={"original_error": str(e)},
            ) from e
        
    async def list_objects_from_s3(
        self,
        prefix: Optional[str],
    ):
        request_id = request_id_ctx_var.get()
        try:
            from src.workers.document.object_storage import list_objects_from_s3_task
            async_result = list_objects_from_s3_task.apply_async(
                args=(prefix,),
                headers={"request_id": request_id},
            )
               
            logger.info("S3 list objects task submitted")
            
            return {
                "task_id": async_result.id,
                "status": async_result.state,
                "message": "S3 list objects task scheduled, use task_id to poll results"
            }
        
        except Exception as e:
            logger.error(f"Failed to list objects from S3: {str(e)}", exc_info=True)
            raise ExternalServiceError(
                service_name="TaskQueue/Celery",
                message="Failed to schedule S3 list objects task",
            ) from e

        
    # async def get_document_stream(
    #     self,
    #     db: AsyncSession,
    #     user_id: UUID,
    #     doc_id: UUID,
    #     background_tasks: BackgroundTasks,
    # ) -> StreamingResponse:
    #     """下载文档内容流"""
    #     try:
    #         from src.workers.document.object_storage import download_document_task
    #         task = download_document_task.apply_async(
    #             args=(str(user_id), str(doc_id)),
    #             headers={"request_id": request_id_ctx_var.get()},
    #         )
            
    #         result = task.get(timeout=300)
    #         if not result or not result.get("content"):
    #             raise NotFoundError(resource="Document content", resource_id=doc_id)
            
    #         # 准备文件流
    #         filename = result["filename"]
    #         file_content = BytesIO(result["content"])
    #         content_type = result.get("content_type", "application/octet-stream")
                
    #         # 使用 BackgroundTasks 确保响应完成后关闭文件流
    #         background_tasks.add_task(file_content.close)
            
    #         # 返回文件流内容
    #         return StreamingResponse(
    #             file_content,
    #             media_type=content_type,
    #             headers={
    #                 "Content-Disposition": f'attachment; filename="{filename}"'
    #             }
    #         )
        
    #     except NotFoundError:
    #         raise  # 直接抛出业务异常
    #     except TimeoutError:
    #         logger.error(f"Download task timeout for document {doc_id}")
    #         raise ExternalServiceError(
    #             service_name="TaskQueue/Celery",
    #             message="Document download task timed out",
    #         ) from e
    #     except Exception as e:
    #         logger.error(f"Unexpected error during download document {doc_id}: {e}", exc_info=True)
    #         raise ExternalServiceError(
    #             service_name="TaskQueue/Celery",
    #             message="Failed to download document",
    #             details={"original_error": str(e)},
    #         )
        
        
    async def get_document_stream(
        self,
        db: AsyncSession,
        user_id: UUID,
        doc_id: UUID,
    ) -> StreamingResponse:
        """下载文档内容流（直接流式转发）"""
        doc = await self.document_crud.get_by_id_async(db, doc_id, user_id)
        if not doc:
            raise NotFoundError(resource="Document", resource_id=doc_id)
        
        if not doc.storage_key:
            raise BusinessLogicError(message="Document has no storage key")
        
        try:
            # 从 MinIO 获取原始响应流（urllib3.response.HTTPResponse）
            minio_response = self.mino_client.get_object(
                storage_key=doc.storage_key
            )
            
            # 定义生成器，分块读取，避免内存溢出
            def stream_generator():
                try:
                    # 每次读取 32KB
                    for chunk in minio_response.stream(32 * 1024):
                        yield chunk
                finally:
                    # 确保流关闭，释放连接
                    minio_response.close()
                    minio_response.release_conn()
            
            # 对文件名进行 URL 编码，纯 ASCII 的字符串，符合 HTTP 头标准
            from urllib.parse import quote
            encoded_filename = quote(doc.filename)
            
            # 返回流式响应  
            return StreamingResponse(
                stream_generator(),
                media_type=doc.content_type or "application/octet-stream",
                headers={
                    # 使用 RFC 5987 标准格式
                    "Content-Disposition": f"attachment; filename*=utf-8''{encoded_filename}",  # 处理非 ASCII 文件名
                    "Content-Length": str(doc.size_bytes) # 告诉前端进度条
                }
            )
        
        except NotFoundError:
            raise
        except BusinessLogicError:
            raise
        except S3Error as e:
            logger.error(f"MinIO S3Error during document download: {e.code} - {e.message}", exc_info=True)
            if e.code == "NoSuchKey":
                raise NotFoundError(resource="File in Storage", resource_id=doc.storage_key) from e
            raise ExternalServiceError(
                service_name="MinIO/S3",
                message="Failed to download document from storage",
                details={"s3_code": e.code, "s3_message": e.message}
            ) from e
        except TimeoutError as e:
            logger.error(f"Download task timeout for document {doc_id}")
            raise ExternalServiceError(
                service_name="MinIO/S3",
                message="Document download task timed out",
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error during download document {doc_id}: {e}", exc_info=True)
            raise ExternalServiceError(
                service_name="MinIO/S3",
                message="Unexpected error during document download",
                details={"original_error": str(e)},
            )
        
    
    async def generate_download_url(
        self, 
        db: AsyncSession,
        user_id: UUID,
        doc_id: UUID,
        expires_in: int = 3600
    ):
        """获取文档的预签名下载链接"""
        try:
            # 权限校验和获取文档元信息
            doc = await self.document_crud.get_by_id_async(db, doc_id, user_id)
            if not doc:
                raise NotFoundError(resource="Document", resource_id=doc_id)
            
            # 生成预签名链接
            minio_client = MinioClient()
            presigned_url = minio_client.get_presigned_url(
                object_name=doc.storage_key,
                expires=timedelta(seconds=expires_in),
                response_headers={
                    "response-content-type": doc.content_type or "application/octet-stream",
                    "response-content-disposition": f"attachement; filename={doc.filename}",
                }
            )
            
            logging.info(f"Generated presigned URL for document {doc_id} by user {user_id}")
            
            return {
                "download_url": presigned_url,
                "expires_in": expires_in,
                "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
                "doc_id": doc_id,
                "filename": doc.filename,
                "size_bytes": doc.size_bytes,
                "content_type": doc.content_type,
            }
        
        except NotFoundError:
            raise  # 直接抛出业务异常
        except S3Error as s3e:
            logger.error(f"MinIO S3Error during presigned URL generation: {s3e.code} - {s3e.message}", exc_info=True)
            raise ExternalServiceError(
                service_name="MinIO/S3",
                message="Failed to generate presigned download URL",
                details={"s3_code": s3e.code, "s3_message": s3e.message}
            ) from s3e
        except (RequestException, ConnectionError, Timeout) as net_exc:
            logger.error(f"Network error during presigned URL generation: {str(net_exc)}", exc_info=True)
            raise ExternalServiceError(
                service_name="MinIO/S3",
                message="Network error during presigned URL generation",
                details={"original_error": str(net_exc)},
            ) from net_exc
        except Exception as e:
            logger.error(f"Failed to generate download URL for document {doc_id}: {str(e)}", exc_info=True)
            raise
        