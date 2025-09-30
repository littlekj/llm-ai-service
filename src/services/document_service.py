from fastapi import UploadFile, HTTPException
from fastapi import Query
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
from uuid import UUID
import aiofiles
import asyncio
import shutil
import os
import tempfile
import uuid
import logging

from src.utils.file_validator import sanitize_filename
from src.utils.file_validator import validate_file_extension
from src.utils.file_validator import validate_file_size_async
from src.schemas.document import DocumentCreate
from src.schemas.document import DocumentResponse
from src.models.document import DocumentStatus
from src.crud.document import document_crud
from src.tasks.document.process_document import upload_document_task
from src.tasks.document.process_document import clean_document_task
from src.models.user import User


logger = logging.getLogger(__name__)

# 获取环境变量
temp_upload_dir = os.getenv("TEMP_UPLOAD_DIR")

# 转换为Path对象
if temp_upload_dir:
    TEMP_UPLOAD_DIR = Path(temp_upload_dir)
else:
    print("tempfile.gettempdir(): ", tempfile.gettempdir())
    TEMP_UPLOAD_DIR = Path(tempfile.gettempdir()) / "llm-ai-service" / "tmp" / "uploads"
    
# 确保目录存在
TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)



# class DocumentService:
    
#     def upload_document(self, file: UploadFile, db: Session, user_id: int):
#         # 校验文件
#         if not file.filename:
#             raise ValueError("File name is required")
#         if not validate_file_extension(file.filename):
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid file extension: {Path(file.filename).suffix}"
#             )
#         try:
#             validate_file_size(file.file)
#         except ValueError as e:
#             error_msg = str(e)
        
#             if "exceeds max limit" in error_msg:
#                 logger.error(f"File size exceeds max limit: {file.filename}")
#                 raise HTTPException(status_code=413, detail=error_msg)
#             elif "cannot be empty" in error_msg:
#                 logger.error(f"File size cannot be empty: {file.filename}")
#                 raise HTTPException(status_code=400, detail="不允许上传空文件")
#             else:
#                 logger.error(f"Unexpected validation error: {error_msg}")
#                 raise HTTPException(status_code=400, detail=error_msg)
        
#         # 创建 Document 文档记录（先不存内容）
#         doc_in = DocumentCreate(
#             filename=file.filename,
#             content_type=file.content_type or "application/octet-stream",
#         )
#         db_doc = document_crud.create_with_user(db, doc_in, user_id)
            
#         # 生成临时路径
#         temp_path = Path(TEMP_UPLOAD_DIR) / f"{db_doc.id}_{int(datetime.now().timestamp())}"
        
#         # 流式写入临时文件
#         try:
#             with open(temp_path, "wb") as f:
#                 shutil.copyfileobj(file.file, f)  # 流式写入文件，节约内存
                
#             # 调用异步任务（传参：文档ID + 临时路径）
#             process_document_task.delay(
#                 document_id=str(db_doc.id), 
#                 temp_file_path=temp_path,
#                 original_filename=file.filename
#             )
            
#             return db_doc
#         except Exception as e:
#             # 清理临时文件
#             if temp_path.exists():
#                 temp_path.unlink()  # 删除已存在的文件
#             document_crud.update_status(db, db_doc, DocumentStatus.FAILED, str(e))
#             raise HTTPException(status_code=500, detail="The file upload failed")
            

class DocumentService:
    
    async def _write_file_stream(self, upload_file: UploadFile, filepath: str):
        """流式写入文件的内部协程"""
        async with aiofiles.open(filepath, "wb") as f:
            while chunk := await upload_file.read(8192):
                await f.write(chunk)
                    
    async def upload_document(
        self, 
        file: UploadFile,
        user: User
    ) -> Dict[str, str]:
        """
        上传文档到对象存储，并创建文档记录
        """
        original_filename = file.filename
        if not original_filename:
            raise HTTPException(status_code=400, detail="File name is required")
        
        original_filename = sanitize_filename(original_filename)
        
        # 校验文件扩展名
        ext = await validate_file_extension(original_filename)
        if not ext:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file extension: {Path(original_filename).suffix}"
            )
            
        # 校验文件大小
        try:
            file_size = await validate_file_size_async(file.file)
            logger.info(f"File size validated: {file_size} bytes, name: {original_filename}")
        except ValueError as e:
            error_msg = str(e)
            if "exceeds" in error_msg:
                raise HTTPException(status_code=413, detail=error_msg)
            elif "empty" in error_msg:
                raise HTTPException(status_code=400, detail="Empty file not allowed")
            else:
                raise HTTPException(status_code=400, detail=f"Invalid file: {error_msg}")
            
        # 生成安全文件名
        safe_filename = f"{uuid.uuid4().hex}{ext}"
        temp_path = TEMP_UPLOAD_DIR / safe_filename
        content_type = file.content_type or "application/octet-stream"
               
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
        
        # 调用 Celery 异步任务
        try:    
            task = upload_document_task.delay(
                user_id=user.id,
                temp_file_path=str(temp_path),
                original_filename=original_filename,
                content_type=content_type,
            )
            print("task:", task)
            logger.info(f"Document processing task scheduled: task_id={task.id}")
            
            return {
                "task_id": task.id, 
                "status": task.state,
                "message": "Document upload successful, processing started in background"
            }

        except Exception as e:
            logger.error(f"Failed to schedule Celery task for {original_filename}: {e}", exc_info=True)
            await asyncio.get_event_loop().run_in_executor(None, temp_path.unlink)
            raise HTTPException(status_code=500, detail="Failed to schedule document processing task")    
        
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
            items, total = await document_crud.get_multi_by_user_async(
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
        
    async def get_document_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ) -> DocumentResponse:
        """根据 ID 获取文档"""
        db_doc = await document_crud.get_by_id_async(db, doc_id, user.id)
        return  DocumentResponse.model_validate(db_doc)
        
    async def delete_soft_by_id(
        self,
        db: AsyncSession,
        doc_id: UUID,
        user: User,
    ):
        """软删除文档"""
        db_doc = await document_crud.delete_soft_with_return_async(db, doc_id, user.id)
        if db_doc is None:
            raise
        storage_key = db_doc.storage_key
         
        clean_task = clean_document_task.delay(doc_id=doc_id, storage_key=storage_key)

        return {
            "task_id": clean_task.id,
            "status": clean_task.state,
            "message": clean_task.result or "Document deletion task scheduled"
        }
            