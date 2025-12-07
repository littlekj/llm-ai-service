from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from typing import Optional, Tuple, List, Dict
from collections.abc import Sequence
from uuid import UUID
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging

from src.models.document import Document, StorageStatus
from src.schemas.document import DocumentCreate
from src.utils.async_utils import run_in_async


logger = logging.getLogger(__name__)

class DocumentCRUD:
        
    # def _get_db_session(self):
    #     if self.use_async:
    #         return get_async_session()
    #     else:
    #         return get_sync_session()
    
    def get_by_id(
        self, 
        db: Session, 
        id: UUID, 
        user_id: UUID
    ) -> Optional[Document]:
        """
        根据 ID 获取文档（软删除过滤）
        """
        doc = db.query(Document).filter(
            Document.id ==id,
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        ).first()
        
        return doc
        
    async def get_by_id_async(
        self, 
        db: AsyncSession, 
        id: UUID, 
        user_id: UUID
    ) -> Optional[Document]:
        """
        根据 ID 获取文档（软删除过滤）
        :param db: 异步数据库会话
        :param id: 文档 ID
        :param user_id: 用户 ID
        :return: 返回文档对象，如果不存在则返回 None
        """
        stmt = select(Document).where(
            Document.id ==id,
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        )
        logger.debug(f"Executinig get_by_id_async query: id={id}, user_id={user_id}")
        result = await db.execute(stmt)
        db_doc = result.scalar_one_or_none()
        
        return db_doc
            
    def get_record_include_soft_delete(
        self,
        db: Session,
        id: UUID,
        user_id: UUID
    ) -> Optional[Document]:
        """根据 ID 获取文档（包含软删除）"""
        stmt = select(Document).where(
            Document.id == id,
            Document.user_id == user_id
        )
        result = db.execute(stmt)
        db_doc = result.scalar_one_or_none()
        
        return db_doc
        
    def get_by_checksum_and_user(
        self,
        db: Session,
        checksum: str,
        user_id: UUID
    ) -> Optional[Document]:
        """
        根据 checksum 和用户 ID 获取文档
        """
        doc = db.query(Document).filter(
            Document.checksum == checksum,
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        ).first()

        return doc
    
    async def get_by_checksum_and_user_async(
        self,
        db: Session,
        checksum: str,
        user_id: UUID
    ) -> Optional[Document]:
        """
        根据 checksum 和用户 ID 获取文档
        """
        stmt = select(Document).where(
            Document.checksum == checksum,
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        )
        
        result = await db.execute(stmt)
        doc = result.scalar_one_or_none()

        return doc
    
    async def get_multi_by_user_async(
        self,
        db: AsyncSession,
        user_id: UUID,
        page: int,
        size: int
    ):
        """
        # 分页查询用户文档列表
        :param db: 异步数据库会话
        :param user_id: 用户 ID
        :param page: 页码（从 1 开始）
        :param size: 每页记录数
        :return: 返回文档列表和总记录数的元组
        """
        offset = (page - 1) * size

        # 查询数据语句
        stmt = (
            select(Document)
            .where(
                Document.user_id == user_id,
                Document.is_deleted == False,
                Document.deleted_at.is_(None)
            )
            .offset(offset).limit(size)
            .order_by(Document.created_at.desc())
        )
        # 查询总数语句
        cnt_stmt = (
            select(func.count())
            .select_from(Document)
            .where(
                Document.user_id == user_id,
                Document.is_deleted == False,
                Document.deleted_at.is_(None)
            )
        )
        
        # 执行数据查询
        result = await db.execute(stmt)
        items = result.scalars().all()  # 返回文档列表：Sequence[Document] 类型
        
        # 执行总数查询
        total_result = await db.execute(cnt_stmt)
        total = total_result.scalar() or 0
        
        return list(items), total 
    
    async def get_multi_with_soft_deleted_async(
        self,
        db: AsyncSession,
        page: int,
        size: int,
    ) -> Tuple[List[Document], int]:
        """
        分页查询软删除的文档
        """
        skip = (page - 1) * size
        
        # 查询数据
        stmt = select(Document).where(
            Document.is_deleted == True,
            Document.deleted_at.isnot(None)
        ).offset(skip).limit(size).order_by(Document.created_at.desc())
        result = await db.execute(stmt)
        
        # 返回文档列表：Sequence[Document] 类型
        items = result.scalars().all()
        
        # 查询总数
        total = len(items)
        
        return list(items), total
    
    def get_soft_deleted_by_id(
        self,
        db: Session,
        id: UUID,
        user_id: UUID,
    ):
        db_doc = db.query(Document).filter(
            Document.id == id,
            Document.user_id == user_id,
            Document.is_deleted == True,
            Document.deleted_at.isnot(None),
        ).first()
        
        return db_doc 
    
    async def get_soft_deleted_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: UUID,
    ):
        stmt = select(Document).where(
            Document.id == id,
            Document.user_id == user_id,
            Document.is_deleted == True,
            Document.deleted_at.isnot(None)
        )
        logger.debug(f"Executing get_soft_deleted_by_id_async query: id={id}, user_id={user_id}")
        result = await db.execute(stmt)
        db_doc = result.scalar_one_or_none()
            
        return db_doc
        
    async def get_by_doc_id_async(
        self,
        db: AsyncSession,
        id: UUID, 
    ) -> Optional[Document]:
        stmt = select(Document).where(
            Document.id ==id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        )
        logger.debug(f"Executinig get_by_id_async query: id={id}")
        result = await db.execute(stmt)
        db_doc = result.scalar_one_or_none()
        
        if db_doc is None:
            logger.info(f"Document not found or already deleted")
            
        return db_doc 
    
    def create_record_with_user_id(
        self,
        db: Session,
        user_id: UUID,
        obj_in: DocumentCreate,
        file_extension: str,
        size_bytes: int,
        checksum: str,
        storage_key: str,
        storage_status: str,
        version_id: str,
    ) -> Document:
        """
        创建文档并关联用户
        """
        db_doc = Document(
            user_id=user_id,
            filename=obj_in.filename,
            content_type=obj_in.content_type,
            file_extension=file_extension,
            size_bytes=size_bytes,
            checksum=checksum,
            storage_key=storage_key,
            storage_status=storage_status,
            version_id=version_id,
        )
        
        db.add(db_doc)
        db.flush()
        db.refresh(db_doc)
        
        return db_doc
    
    def update_record_for_doc(
        self,
        db: Session,
        doc: Document,
        file_extension: str,
        size_bytes: int,
        content_type: str,
        storage_key: str,
        storage_status: str,
        version_id: str,
        doc_metadata: Dict[str, any]
    ) -> Document:
        
        doc.file_extension = file_extension
        doc.size_bytes = size_bytes
        doc.content_type = content_type
        doc.storage_key = storage_key
        doc.storage_status = storage_status
        doc.version_id = version_id
        doc.doc_metadata = doc_metadata
        
        db.flush()
        db.refresh(doc)
        
        return doc
    
    def update_status(
        self,
        db: Session,
        doc: Document,
        storage_status: Optional[str] = None,
        version_id: Optional[str] = None,
    ) -> Document:
        """
        更新文档状态
        """
        if storage_status:
            doc.storage_status = storage_status
        if version_id:
            doc.version_id = version_id
        
        db.commit()
        db.flush(doc)
        
        return doc
    
    def soft_delete(
        self,
        db: Session,
        doc: Document,
        deleted_at: datetime,
        updated_at: datetime,
        version_id: Optional[str],
    ):
        doc.storage_status = StorageStatus.DELETED
        doc.is_deleted = True
        doc.deleted_at = deleted_at
        doc.updated_at = updated_at
        doc.version_id = version_id
        db.flush()
        db.refresh(doc)
        
        return doc
        
    def restore(
        self,
        db: Session,
        doc: Document,
        deleted_at: Optional[datetime],
        updated_at: datetime,
        version_id: Optional[str],
    ):
        """恢复软删除的文档"""
        doc.storage_status = StorageStatus.ACTIVE
        doc.is_deleted = False
        doc.deleted_at = deleted_at
        doc.updated_at = updated_at
        doc.version_id = version_id
        
        db.flush()
        db.refresh(doc)
        
        return doc
    
    def permanent_delete_by_id(
        self,
        db: Session,
        id: UUID,
        user_id: UUID,
    ):
        """永久删除文档记录"""
        stmt = delete(Document).where(
            Document.id == id,
            Document.user_id == user_id
        )
        db.execute(stmt)
        db.flush()
        
        return True
        
    def get_expired_soft_deleted(
        self,
        db: Session,
        cutoff_date: datetime,
        limit: int = 100,  # 添加批量限制
    ) -> List[Document]:
        """获取超过保留期限的软删除文档"""
        stmt = select(Document).where(
            Document.is_deleted == True,
            Document.deleted_at.isnot(None),
            Document.deleted_at <= cutoff_date
        ).order_by(
            Document.deleted_at.asc()
        ).limit(limit)
        
        result = db.execute(stmt)
        items = result.scalars().all()
        
        if not items:
            return []
        
        return list(items)