import uuid
import shutil
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Depends
from fastapi import Query
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import logging

from src.models.user import User
from src.schemas.document import DocumentResponse
from src.schemas.document import PaginationResponse
from src.schemas.document import create_pagination_response
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.core.depends import get_document_service
from src.services.document_service import DocumentService


logger = logging.getLogger(__name__)


UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt",
                      ".md", ".ppt", ".png", ".jpg", ".jpeg"}

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("/")
async def upload_document(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service),
):
    # 校验用户身份
    if not current_user or not current_user.id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # 上传文档
    result = await document_service.upload_document(file, current_user)
    
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



@router.get("/{doc_id}", response_model=DocumentResponse)
async def get_document_by_id(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    document_service: DocumentService = Depends(get_document_service)
):
    
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
    
    result = await document_service.delete_soft_by_id(db, doc_id, current_user)
    
    if not result:
        raise HTTPException(status_code=404, detail="Document not found or not authorized")
    return {
        "doc_id": str(doc_id),
        "message": "The document has been successfully deleted",
    }

    
    

# @router.delete("/{doc_id}")
# def remove_document(doc_id: int, db: Session = Depends(get_db_session)):
#     # 查询文档是否存在
#     doc = crud.get_document(db, doc_id)
#     if not doc:
#         raise HTTPException(status_code=404, detail="Document not found")
    
#     file_path = Path(doc.filepath)
    
#     # 尝试删除物理文件
#     if file_path.exists():
#         try:
#             file_path.unlink()
#         except Exception as e:
#             logger.error(f"Failed to delete file: {file_path}: {e}")
#             raise HTTPException(
#                 status_code=500, 
#                 detail="Failed to delete physical file. Please retry or contact admin."
#             )
#     else:
#         # 如果文件不存在，记录日志并继续删除数据库记录
#         logger.warning(f"File not found for doc {doc_id}: {file_path}")
        
#     # 删除数据库记录
#     deleted_doc = crud.delete_document(db, doc_id)
#     if not deleted_doc:
#         logger.error(f"Failed to delete database record for doc {doc_id} after file deletion")
#         raise HTTPException(
#             status_code=500,
#             detail="Database inconsistency detected. Please retry or contact admin."
#         )
    
#     return {"message": "Document deleted successfully"}
