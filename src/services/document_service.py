import aiofiles
import asyncio
import shutil
import os
import tempfile
import uuid
import logging
from fastapi import UploadFile, HTTPException
from fastapi import Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
from uuid import UUID
from io import BytesIO
from fastapi.responses import StreamingResponse
from fastapi import BackgroundTasks
from datetime import timezone, timedelta

from src.utils.file_validator import sanitize_filename
from src.utils.file_validator import validate_file_extension
from src.utils.file_validator import validate_file_size_async
from src.schemas.document import DocumentCreate
from src.schemas.document import DocumentResponse
from src.schemas.document import DocumentObjectResponse
from src.models.user import User
from src.crud.document import DocumentCRUD
from src.workers.document.process_document import upload_document_task
from src.workers.document.process_document import soft_delete_document_task
from src.workers.document.process_document import restore_document_task
from src.workers.document.process_document import permanent_delete_document_task
from src.workers.document.process_document import permanent_delete_from_s3_task
from src.workers.document.process_document import list_objects_from_s3_task
from src.workers.document.process_document import download_document_task
from src.utils.minio_storage import get_minio_client
from src.middleware.request_id import request_id_ctx_var
from src.core.errors import (
    FileValidationError,
    StorageServiceError,
    DatabaseError,
    TaskQueueError,
    ErrorCode,
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
    def __init__(
        self,
        document_crud: DocumentCRUD
    ):
        self.document_crud = document_crud
                    
    async def upload_document(
        self, 
        file: UploadFile,
        user: User
    ) -> Dict[str, str]:
        """
        上传文档到对象存储，并创建文档记录
        """
        # 从上下文获取 request_id
        request_id = request_id_ctx_var.get()
        logger.info(f"Starting document upload")
        
        # ===== 文件验证 =====
        try:
            filename, file_ext = await self._validate_file(file)
        except FileValidationError:
            raise  # 直接向上抛出业务异常
        except Exception as e:
            logger.error(f"Unexcepted error during file validation: {e}", exc_info=True)
            raise FileValidationError(
                message="File validation failed",
                error_code=ErrorCode.FILE_CORRUPTED,
                details={"filename": file.filename}
            ) from e  # 链式异常抛出时保留原始异常信息

        # 生成安全文件名
        safe_filename = f"{uuid.uuid4().hex}{file_ext}"
        temp_path = TEMP_UPLOAD_DIR / safe_filename
               
        # 异步流式写入临时文件      
        try:
            await asyncio.wait_for(
                self._write_file_stream(file, str(temp_path)),
                timeout=30.0  # 设置超时时间，防止文件写入时间过长
            ) 
            logger.info(f"Temporary file written: {temp_path}, size: {temp_path.stat().st_size}") 
          
        except TimeoutError:
            logger.error(f"File write timeout: {file.filename}")  
            raise HTTPException(status_code=408, detail="Upload timeout")
        except Exception as e:
            logger.error(f"Failed to write temporary file {temp_path}: {e}", exc_info=True)
            # 清理临时文件
            if temp_path.exists():
                await asyncio.get_event_loop().run_in_executor(None, temp_path.unlink)  # 删除已存在的文件
            raise HTTPException(status_code=500, detail="The temporary file write failed")
        
        # ==== 提交 Celery 上传文档任务 ====
        try:  
            # task = upload_document_task.delay(
            #     user_id=user.id,
            #     temp_file_path=str(temp_path),
            #     original_filename=original_filename,
            # )
            
            # 设置消息 headers、 routing/queue/eta 等任务选项使用 apply_async(...)
            task = upload_document_task.apply_async(
                kwargs={
                    "user_id": user.id,
                    "temp_file_path": str(temp_path),
                    "original_filename": filename,
                },
                headers={"request_id": request_id} if request_id else None,
            )
            
            logger.info("Upload document task submitted")
            
            return {
                "task_id": task.id, 
                "status": task.state,
                "message": "Document upload task has been scheduled in background"
            }

        except Exception as e:
            logger.error(f"Failed to submit document upload task: {e}", exc_info=True)
            # 失败时清理临时文件
            try:
                if temp_path.exists():
                    await asyncio.get_event_loop().run_in_executor(None, temp_path.unlink)
            except Exception as ce:
                logger.error(f"Failed to clean up temp file {temp_path}: {ce}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to schedule document upload task") 
        
    async def _validate_file(self, file: UploadFile):
        """文件验证"""
        # 检查文件是否为空
        if not file or not file.filename:
            raise FileValidationError(
                message="No file provided",
                error_code=ErrorCode.FILE_EMPTY,
            )
        
        # 清理并获取安全文件名
        filename = sanitize_filename(file.filename)
            
        # 检查文件类型
        try:
            file_ext = await validate_file_extension(filename)
        except FileValidationError as e:
            # 异常被全局异常处理器捕获并返回格式化的错误响应
            raise
        
        # 检查文件大小
        try:
            await validate_file_size_async(file.file)
        except FileValidationError as e:
            raise
        
        return filename, file_ext
        
    async def _write_file_stream(self, upload_file: UploadFile, filepath: str):
        """流式写入文件的内部协程"""
        async with aiofiles.open(filepath, "wb") as f:
            while chunk := await upload_file.read(8192):
                await f.write(chunk)
        
        
        
    async def list_documents(
        self,
        db: AsyncSession,
        user: User,
        page: int = Query(1, ge=1),           # 默认从第一页开始
        size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    ) -> tuple[List[DocumentResponse], int]:
        """
        列出用户的所有文档
        """
        skip = (page - 1) * size
        
        try:
            items, total = await self.document_crud.get_multi_by_user_async(
                db=db, 
                user_id=user.id, 
                skip=skip, 
                limit=size
            )
            
            # 将 ORM 模型转换为 Pydantic 响应模型
            items = [DocumentResponse.model_validate(item) for item in items]
            
            return items, total

        except Exception as e:
            logger.error(f"Error listing documents for user {user.id}: {e}", exc_info=True)
            raise
        
    async def list_documents_with_soft_deleted(
        self,
        db: AsyncSession,
        page: int = Query(1, ge=1),           # 默认从第一页开始
        size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    ) -> tuple[List[DocumentResponse], int]:
        """
        列出所有软删除的文档
        """
        skip = (page - 1) * size
        
        try:
            items, total = await self.document_crud.get_multi_with_soft_deleted_async(
                db=db,
                skip=skip,
                limit=size,
            )
            
            # 将 ORM 模型转换为 Pydantic 响应模型
            items = [DocumentResponse.model_validate(item) for item in items]
            
            return items, total
        except Exception as e:
            logger.error(f"Error listing documents with soft deleted: {e}", exc_info=True)
            raise
        
    async def get_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ) -> DocumentResponse:
        """根据 ID 获取文档"""
        db_doc = await self.document_crud.get_by_id_async(db, doc_id, user.id)
        return  DocumentResponse.model_validate(db_doc)
        
    async def soft_delete_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        """软删除文档"""
        db_doc = await self.document_crud.get_by_id_async(db, doc_id, user.id)
        if not db_doc:
            raise HTTPException(status_code=404, detail="Document not found")
        if db_doc.is_deleted:
            raise HTTPException(status_code=400, detail="Document already deleted")
        
        soft_delete_task = soft_delete_document_task.delay(doc_id=doc_id, user_id=user.id)
        
        return {
            "status": soft_delete_task.state,
            "task_id": soft_delete_task.id,
            "message": "Document soft deletion task scheduled"
        }
            
    async def restore_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        db_doc = await self.document_crud.get_soft_deleted_by_id_async(db, doc_id, user.id)
        if not db_doc:
            raise HTTPException(status_code=404, detail="Soft deleted document not found")
        if not db_doc.is_deleted:
            raise HTTPException(status_code=400, detail="Document not soft deleted")
        if not db_doc.version_id:
            raise HTTPException(status_code=400, detail="Document has no version for restoration")
        
        print("db_doc.version_id: ", db_doc.version_id)
        
        restore_task = restore_document_task.delay(
            doc_id=doc_id, 
            user_id=user.id, 
            version_id=db_doc.version_id
        )
        
        return {
            "status": restore_task.state,
            "task_id": restore_task.id,
            "message": "Document restoration task scheduled"
        }
        
    async def permanently_delete_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        try:
            
            result = permanent_delete_document_task.delay(user.id, doc_id)
            
            return {
                "status": result.state,
                "task_id": result.id,
                "message": "Document deletion task scheduled"
            }
        
        except Exception as e:
            logger.error(f"Failed to permanently delete document: {e}", exc_info=True)
            raise
        
    async def permanently_delete_from_s3(
        self,
        # db: AsyncSession,
        user: User,
        storage_key: str,
    ):
        try:
            result = permanent_delete_from_s3_task.delay(user.id, storage_key)
            
            return {
                "status": result.state,
                "task_id": result.id,
                "message": result.result or "Document deletion task scheduled"
            }

        except Exception as e:
            logger.error(f"Failed to permanently delete document from S3: {e}", exc_info=True)
            raise
        
    async def list_objects_from_s3(
        self,
        prefix: Optional[str],
    ):
        try:
            request_id = request_id_ctx_var.get()
            async_result = list_objects_from_s3_task.apply_async(
                args=(prefix,),
                headers={"request_id": request_id},
            )
            
            # async_result = async_result.get(timeout=15)
            # doc_objects = [DocumentObjectResponse.model_validate(obj) for obj in async_result['objects']]
            # return doc_objects, len(doc_objects)
               
            logger.info("S3 list objects task submitted")
            
            return {
                "task_id": async_result.id,
                "status": async_result.state,
                "message": "S3 list objects task scheduled, use task_id to poll results"
            }
        
        except Exception as e:
            logger.error(f"Failed to list objects from S3: {e}", exc_info=True)
            raise TaskQueueError(
                message="Failed to schedule S3 list objects task",
                error_code=ErrorCode.TASK_QUEUE_ERROR
            ) from e
        
    async def get_document_stream(
        self,
        db: AsyncSession,
        user_id: UUID,
        doc_id: UUID,
        background_tasks: BackgroundTasks,
    ) -> StreamingResponse:
        """下载文档内容流"""
        try:
            task = download_document_task.apply_async(
                args=(user_id, doc_id),
                headers={"request_id": request_id_ctx_var.get()},
            )
            
            result = task.get(timeout=30)
            if not result or not result.get("content"):
                raise ValueError("Failed to retrieve document content")
            
            filename = result["filename"]
            file_content = BytesIO(result["content"])
            
            content_type = result.get("content_type", "application/octet-stream")
            
            # 使用 BackgroundTasks 确保响应完成后关闭文件流
            background_tasks.add_task(file_content.close)
            
            # 返回文件流内容
            return StreamingResponse(
                file_content,
                media_type=content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"'
                }
            )
            
        except Exception as e:
            logger.error(f"Error scheduling download task for document {doc_id}: {e}", exc_info=True)
            raise
        
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
            document_crud = DocumentCRUD()
            doc = await document_crud.get_by_id_async(db, doc_id, user_id)
            if not doc:
                raise HTTPException(status_code=404, detail="Document not found or already deleted")
            
            minio_client = get_minio_client()
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
        
        except HTTPException:
            raise    
        except Exception as e:
            logger.error(f"Error scheduling download URL task for document {doc_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to generate download URL")