import uuid
import shutil
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Depends
from fastapi import Query
from fastapi import Request, HTTPException
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from typing import Optional
import logging

from src.models.user import User
from src.schemas.document import DocumentResponse
from src.schemas.document import PaginationResponse
from src.schemas.document import DocumentObjectResponse, DocumentObjectPaginationResponse
from src.schemas.document import create_pagination_response
from src.schemas.pagination import create_pagination_response as create_pagination
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.core.depends import get_document_service
from src.services.document_service import DocumentService
from src.middleware.request_id import request_id_ctx_var
from src.crud.document import DocumentCRUD

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
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    # 上传文档（异常由全局处理器统一处理）
    logger.info("Document upload request received", extra={"user_id": str(current_user.id)})
    
    result = await document_service.upload_document(file, current_user)
    
    logger.info("Document upload processed", extra={"user_id": str(current_user.id)})
    
    return result


@router.get("/", response_model=PaginationResponse[DocumentResponse])
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
    try:
        items, total = await document_service.list_documents(
            db=db,
            user=current_user,
            page=page,
            size=size
        )
        
        return create_pagination_response(items, total, page, size)
    
    except Exception as e:
        logger.error(f"Failed to list documents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/soft-deleted", response_model=PaginationResponse[DocumentResponse])
async def list_documents_with_soft_deleted(
    db: AsyncSession = Depends(get_async_session),
    page: int = Query(1, ge=1),           # 默认从第一页开始
    size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    document_service: DocumentService = Depends(get_document_service),
):
    try:
        items, total = await document_service.list_documents_with_soft_deleted(
            db=db,
            page=page,
            size=size,
        )
        
        return create_pagination_response(items, total, page, size)
        
    except Exception as e:
        logger.error(f"Failed to list documents with soft deleted: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# response_model=DocumentObjectPaginationResponse[DocumentObjectResponse]
@router.get("/objects")
async def list_objects_from_s3(
    # db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    prefix: Optional[str] = Query(None, description="S3 object prefix"),
    # page: int = Query(1, ge=1),  # 默认从第一页开始
    # size: int = Query(10, ge=1, le=100),  # 默认每页显示10条，限制最大值，防滥用
    document_service: DocumentService = Depends(get_document_service),
):
    if not current_user or not current_user.id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    logger.info(f"Listing objects from S3 for user {current_user.id}")
    try:
        # items, total = await document_service.list_objects_from_s3(prefix)
        
        # return create_pagination(
        #     items, total, page, size, 
        #     response_model=DocumentObjectPaginationResponse[DocumentObjectResponse]
        # )
        
        return await document_service.list_objects_from_s3(prefix)
    
    except Exception as e:
        logger.error(f"Failed to list objects from S3: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{doc_id}", response_model=DocumentResponse)
async def get_document_by_id(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service)
):
    """获取指定文档的详细信息"""
    try:
        doc = await document_service.get_document_by_id(db, doc_id, current_user)
    
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found or already deleted")
        return doc
    
    except Exception as e:
        logger.error(f"Failed to get document {doc_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    

@router.delete("/{doc_id}")
async def remove_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service)
):
    
    result = await document_service.soft_delete_document_by_id(db, doc_id, current_user)
    
    if not result:
        raise HTTPException(status_code=404, detail="Document not found or not authorized")
    
    return {
        "doc_id": doc_id,
        "message": "Document soft deletion processed",
    }

@router.put("/{doc_id}")
async def restore_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    if not current_user or not current_user.id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    result = await document_service.restore_document_by_id(db, doc_id, current_user)
    
    if not result:
        raise HTTPException(status_code=404, detail="Document not found or not authorized")
    
    return {
        "doc_id": doc_id,
        "message": "Document restoration processed",
    }
    
@router.delete("/permanently/{doc_id}")
async def permanently_delete_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    if not current_user or not current_user.id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    result = await document_service.permanently_delete_document_by_id(db, doc_id, current_user)
    
    if not result:
        raise HTTPException(status_code=404, detail="Document not found or not authorized")
    
    return {
        "doc_id": doc_id,
        "message": "Document permanently deletion processed",
    }
    
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
    
    if not current_user or not current_user.id:
        logger.warning(f"Unauthorized deletion attempt for storage key {storage_key}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        result = await document_service.permanently_delete_from_s3(current_user, storage_key)
        if not result:
            raise HTTPException(status_code=404, detail="Document not found or not authorized")
        return {
            "storage_key": str(storage_key),
            "message": "Document permanently deletion processed from S3",
        }
    except Exception as e:
        logger.error(f"Error deleting file from S3: {e}")
        raise HTTPException(status_code=500, detail="Error deleting file from S3")
   
@router.get("/{doc_id}/download") 
async def download_document(
    doc_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service), 
):
    try:
        # 校验用户身份
        if not current_user or not current_user.id:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        # 获取文件流
        return await document_service.get_document_stream(
            db=db,
            user_id=current_user.id,
            doc_id=doc_id,
            background_tasks=background_tasks
        )
        
    except ValueError as e:
        logger.warning(f"Download validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during download document {doc_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error during download document")
    
@router.get("/{doc_id}/download-url")
async def get_document_download_url(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    try:
        # 获取预签名下载链接
        return await document_service.generate_download_url(
            db=db,
            user_id=current_user.id,
            doc_id=doc_id, 
            expires_in=3600
        )
        
    except ValueError as e:
        logger.warning(f"Get download URL validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during get download URL for document {doc_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error during get download URL")