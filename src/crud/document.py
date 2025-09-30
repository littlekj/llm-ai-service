from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import Optional, Tuple, List
from collections.abc import Sequence
from uuid import UUID
from sqlalchemy.exc import SQLAlchemyError
import logging

from src.models.document import Document
from src.schemas.document import DocumentCreate, DocumentStatus
from src.utils.async_utils import run_in_async


logger = logging.getLogger(__name__)

class DocumentCRUD:
    # def __init__(self, use_async: bool = True):
    #     self.use_async = use_async
        
    # def _get_db_session(self):
    #     if self.use_async:
    #         return get_async_session()
    #     else:
    #         return get_sync_session()
    
    async def get_by_id(
        self, 
        db: Session, 
        id: UUID, 
        user_id: int
    ) -> Optional[Document]:
        """
        根据 ID 获取文档（软删除过滤）
        """
        doc = db.query(Document).filter(
            Document.id ==id,
            Document.user_id == user_id,
            Document.deleted_at.is_(None)
        ).first()
        
        return doc
        
    async def get_by_id_async(
        self, 
        db: AsyncSession, 
        id: UUID, 
        user_id: int
    ) -> Optional[Document]:
        """
        根据 ID 获取文档（软删除过滤）
        :param db: 异步数据库会话
        :param id: 文档 ID
        :param user_id: 用户 ID
        :return: 返回文档对象，如果不存在则返回 None
        """
        try:
            stmt = select(Document).where(
                Document.id ==id,
                Document.user_id == user_id,
                Document.deleted_at.is_(None)
            )
            logger.debug("Executinig get_by_id_async query: id={id}, user_id={user_id}")
            result = await db.execute(stmt)
            db_doc = result.scalar_one_or_none()
            
            if db_doc is None:
                logger.info(f"Document not found or already deleted: id={id}")
                
            return db_doc
    
        except SQLAlchemyError as e:
            logger.error(f"Database error in get_by_id_async query: id={id}, error={str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_by_id_async query: id={id}, error={str(e)}", exc_info=True)
            raise
    
    def get_multi_by_user(
        self,
        db: Session,
        user_id: int,
        skip: int = 0,
        limit: int = 20
    ) -> Tuple[List[Document], int]:
        """
        分页查询用户文档列表
        """
        # 查询数据
        query = db.query(Document).filter(
            Document.user_id == user_id,
            Document.deleted_at.is_(None)
        )
        total = query.count()
        items = query.offset(skip).limit(limit).all()
        return list(items), total
    
    async def get_multi_by_user_async(
        self,
        db: AsyncSession,
        user_id: int,
        skip: int = 0,
        limit: int = 20
    ) -> Tuple[List[Document], int]:
        """
        分页查询用户文档列表
        :param db: 异步数据库会话
        :param user_id: 用户 ID
        :param skip: 跳过记录数
        :param limit: 每页记录数
        :return: 返回文档列表和总记录数的元组
        """
        # 查询数据
        stmt = select(Document).where(
            Document.user_id == user_id,
            Document.deleted_at.is_(None)
        ).offset(skip).limit(limit).order_by(Document.created_at.desc())  # 分页和排序
        result = await db.execute(stmt)
        
        # 返回文档列表：Sequence[Document] 类型
        items = result.scalars().all()  
        
        # 查询总数
        count_stmt = select(func.count()).where(
            Document.user_id == user_id,
            Document.deleted_at.is_(None)
        )
        total_result = await db.execute(count_stmt)
        total = total_result.scalar_one()
    
        return list(items), total    
    
    def create_record_with_user_id(
        self,
        db: Session,
        user_id: int,
        obj_in: DocumentCreate,
        storage_key: str,
        size_bytes: int,
        status: DocumentStatus,
        checksum: str,
    ) -> Document:
        """
        创建文档并关联用户
        """
        db_doc = Document(
            filename=obj_in.filename,
            storage_key=storage_key,
            size_bytes=size_bytes,
            checksum=checksum,
            content_type=obj_in.content_type,
            status=status,
            user_id=user_id,
        )
        
        db.add(db_doc)
        db.commit()
        db.refresh(db_doc)
        return db_doc
    
    async def create_record_with_user_id_async(
        self,
        db: AsyncSession,
        user_id: int,
        obj_in: DocumentCreate,
        storage_key: str,
        size_bytes: int,
        status: DocumentStatus,
        checksum: str,
    ) -> Document:
        """
        创建文档并关联用户
        """
        db_doc = Document(
            filename=obj_in.filename,
            storage_key=storage_key,
            size_bytes=size_bytes,
            checksum=checksum,
            content_type=obj_in.content_type,
            status=status,
            user_id=user_id,
        )
        
        db.add(db_doc)
        await db.commit()
        await db.refresh(db_doc)
        return db_doc
        
    
    def update_status(
        self,
        db: Session,
        doc: Document,
        status: DocumentStatus,
        error_message: Optional[str] = None
    ) -> Document:
        """
        更新文档状态
        """
        doc.status = status
        if error_message:
            doc.error_message = error_message[:500]  # 截断防溢出
        db.commit()
        db.refresh(doc)
        return doc
    
    async def update_status_async(
        self,
        db: AsyncSession,
        doc: Document,
        status: DocumentStatus,
        error_message: Optional[str] = None
    ) -> Document:
        """
        更新文档状态
        """
        doc.status = status
        if error_message:
            doc.error_message = error_message[:500]  # 截断防溢出
        await db.commit()
        await db.refresh(doc)
        return doc
                
    async def update_status_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: int,
        status: DocumentStatus,
        error_message: Optional[str] = None
    ) -> bool:
        """
        直接通过 ID 更新文档状态（更高效，避免先 SELECT）
        """
        stmt = update(Document).where(
            Document.id == id,
            Document.user_id == user_id,
            Document.deleted_at.is_(None)
        ).values(
            status=status,
            error_message=error_message[:500] if error_message else None  # 截断防溢出
        )
        
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount > 0

    async def delete_soft_with_return_async(
            self, 
            db: AsyncSession, 
            id: UUID, 
            user_id: int
        ) -> Optional[Document]:
        """
        软删除文档（标记删除时间）
        """
        try:
            # 先查后删（适合需要触发事件或校验的场景）
            db_doc = await self.get_by_id_async(db, id, user_id)
            if not db_doc:
                return None
            
            if db_doc.status == DocumentStatus.PROCESSING:
                raise ValueError("Cannot delete the document that is being processed")
            
            stmt_update = update(Document).where(
                Document.id == id,
            ).values(
                deleted_at=func.now()
            )
            await db.execute(stmt_update)
            await db.commit()
            
            return db_doc  # 返回被删除的文档对象（未刷新）
        
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during soft delete document {id}")
            raise  # 向上抛出异常，让调用者处理
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during soft delete document {id}")
            raise  # 向上抛出异常，让调用者处理
        
    
    async def delete_soft_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: int
    ) -> bool:
        """软删除文档（直接通过 ID 更新）"""
        try:
            stmt = update(Document).where(
                Document.id == id,
                Document.user_id == user_id,
                Document.deleted_at.is_(None)
            ).values(
                deleted_at=func.now()
            )
            result = await db.execute(stmt)
            await db.commit()
            
            deleted_count = result.rowcount
            if deleted_count == 0:
                logger.warning(f"Soft delete failed, may have been deleted or has no permissions, id: {id}")
            else:
                logger.info(f"Soft delete success, document id: {id}")
            
            return deleted_count > 0
    
        except SQLAlchemyError as e:
            logger.error(f"Database error during soft delete document {id}", str(e))
            await db.rollback()
            raise  # 向上抛出异常，让调用者处理
        except Exception as e:
            logger.error(f"Unexpected error during soft delete document {id}", str(e))
            await db.rollback()
            raise  # 向上抛出异常，让调用者处理


# 使用依赖注入   
document_crud = DocumentCRUD()