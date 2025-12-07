from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Depends
from fastapi import Query
from fastapi import Request, HTTPException
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from typing import Optional
from celery import chain
import logging

from src.models.user import User
from src.schemas.document import DocumentDetailResponse
from src.schemas.document import PaginationResponse
from src.schemas.document import create_pagination_response
from src.schemas.pagination import create_pagination_response as create_pagination
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.core.depends import get_document_service
from src.crud.document import DocumentCRUD
from src.services.document_service import DocumentService
from src.middleware.request_id import request_id_ctx_var
from src.workers.document.object_storage import upload_document_task
from src.workers.document.vector_storage import process_document_task

logger = logging.getLogger(__name__)


UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt",
                      ".md", ".ppt", ".png", ".jpg", ".jpeg"}

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("/")
async def upload_document(
    request: Request,
    upload_file: UploadFile = File(...),
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
    auto_vectorize: bool = Query(default=True, description="是否自动向量化文档"),  # 可选参数
):
    """
    上传文档并自动处理
    
    - 自动执行：文件上传 -> 文本提取 -> 向量化及存储
    - 返回： 任务 ID（用于轮询进度）
    """
    request_id = request_id_ctx_var.get()

    # 上传并处理文档（异常由全局处理器统一处理）
    logger.info("Document upload request received from user {current_user.id}.")
    
    # 文件预取处理
    filename, temp_path = await document_service.preprocessing_file(upload_file=upload_file)
    
    if auto_vectorize:
        # 提交 Celery 任务链
        upload_sig = upload_document_task.si(
            user_id=str(current_user.id),
            filename=filename,
            temp_path=str(temp_path)
        )
        process_sig = process_document_task.s()  # 接收上一个任务的返回值
        
        if request_id:
            headers = {"request_id": request_id}
            upload_sig.set(headers=headers)
            process_sig.set(headers=headers)
            
        task_chain = chain(upload_sig, process_sig) 
        result = task_chain.apply_async()
    
    else:
        # 仅上传文档，不自动提取文本和向量化
        # 设置消息 headers、 routing/queue/eta 等任务选项使用 apply_async(...)
        result = upload_document_task.apply_async(
            kwargs={
                "user_id": str(current_user.id),
                # "doc_id": str(existing_doc.id) if existing_doc else None,
                "filename": filename,
                "temp_path": str(temp_path),
            },
            headers={"request_id": request_id} if request_id else None,
        )
          
    logger.info(f"Document process task submitted: {result.id}.")
    
    return {
        "task_id": result.id,
        "status": result.state,
        "message": "Document process task submitted in background."        
    }


@router.get("/", response_model=PaginationResponse[DocumentDetailResponse])
async def list_documents(
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    page: int = Query(1, ge=1),           # 默认从第一页开始
    size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    document_service: DocumentService = Depends(get_document_service),
):
    """
    获取当前用户的文档列表
    """
    logger.info("List documents request", extra={"user_id": str(current_user.id)})
  
    items, total = await document_service.list_documents(
        db=db,
        user=current_user,
        page=page,
        size=size
    )
    
    return create_pagination_response(items, total, page, size)


@router.get("/soft-deleted", response_model=PaginationResponse[DocumentDetailResponse])
async def list_documents_with_soft_deleted(
    db: AsyncSession = Depends(get_async_session),
    page: int = Query(1, ge=1),           # 默认从第一页开始
    size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    document_service: DocumentService = Depends(get_document_service),
):
    """列出所有软删除的文档"""
    logger.info("List documents with soft deleted request")
    
    items, total = await document_service.list_documents_with_soft_deleted(
        db=db,
        page=page,
        size=size,
    )
    
    logger.info(f"List documents with soft deleted processed: {total} items found")
    
    return create_pagination_response(items, total, page, size)

# response_model=DocumentObjectPaginationResponse[DocumentObjectResponse]
@router.get("/objects")
async def list_objects_from_s3(
    prefix: Optional[str] = Query(None, description="S3 object prefix"),
    document_service: DocumentService = Depends(get_document_service),
):
    """列出 S3 存储中的对象列表"""
    logger.info(f"Listing objects from S3 with prefix: {prefix}")
    
    # items, total = await document_service.list_objects_from_s3(prefix)
        
        # return create_pagination(
        #     items, total, page, size, 
        #     response_model=DocumentObjectPaginationResponse[DocumentObjectResponse]
        # )
        
    return await document_service.list_objects_from_s3(prefix)


@router.get("/{doc_id}", response_model=DocumentDetailResponse)
async def get_document_by_id(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service)
):
    """获取指定文档的详细信息"""
    logger.info("Get document by ID request", extra={"doc_id": str(doc_id)})
    
    doc = await document_service.get_document_by_id(db, doc_id, current_user)
    
    return doc


@router.delete("/{doc_id}")
async def remove_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service)
):
    logger.info("Soft delete document request", extra={"doc_id": str(doc_id)})
    
    return await document_service.soft_delete_document_by_id(db, doc_id, current_user)


@router.put("/{doc_id}")
async def restore_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    logger.info("Restore document request", extra={"doc_id": str(doc_id)})
    
    return await document_service.restore_document_by_id(db, doc_id, current_user)

    
@router.delete("/permanently/{doc_id}")
async def permanently_delete_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    logger.info("Permanently delete document request", extra={"doc_id": str(doc_id)})
    
    return await document_service.permanently_delete_document_by_id(db, doc_id, current_user)
    
    
@router.delete("/permanently/s3/{storage_key:path}")
async def permanently_delete_from_s3(
    storage_key: str,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    """
    永久删除文档
    - storage_key: MinIO 存储路径，例如 uploads/2025/10/29/xxx.txt
    """
    request_id = request_id_ctx_var.get()
    logger.info(f"[{request_id}] Attempting to permanently delete document: {storage_key}")
    
    return await document_service.permanently_delete_from_s3(current_user, storage_key)

   
@router.get("/{doc_id}/download") 
async def download_document(
    doc_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service), 
):
    """下载文档"""
    
    logger.info("Download document request", extra={"doc_id": str(doc_id)})
    
    return await document_service.get_document_stream(
        db=db,
        user_id=current_user.id,
        doc_id=doc_id,
        background_tasks=background_tasks
    )
    
@router.get("/{doc_id}/download-url")
async def get_document_download_url(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    """获取文档的预签名下载链接"""
    logger.info("Get document download URL request", extra={"doc_id": str(doc_id)})
    
    return await document_service.generate_download_url(
        db=db,
        user_id=current_user.id,
        doc_id=doc_id, 
        expires_in=3600
    )
