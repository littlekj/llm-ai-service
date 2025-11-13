from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from typing import Optional, Tuple, List
from collections.abc import Sequence
from uuid import UUID
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging

from src.models.document import Document, StorageStatus, ProcessingStatus
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
        try:
            stmt = select(Document).where(
                Document.id ==id,
                Document.user_id == user_id,
                Document.is_deleted == False,
                Document.deleted_at.is_(None)
            )
            logger.debug(f"Executinig get_by_id_async query: id={id}, user_id={user_id}")
            result = await db.execute(stmt)
            db_doc = result.scalar_one_or_none()
            
            if db_doc is None:
                logger.info(f"Document not found or already deleted: id={id}")
                
            return db_doc
    
        except SQLAlchemyError as e:
            logger.error(f"Database query error: id={id}, error={str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query error: id={id}, error={str(e)}", exc_info=True)
            raise
        
    def get_record_include_soft_delete(
        self,
        db: Session,
        id: UUID,
        user_id: UUID
    ) -> Optional[Document]:
        """根据 ID 获取文档（包含软删除）"""
        try:
            stmt = select(Document).where(
                Document.id == id,
                Document.user_id == user_id
            )
            result = db.execute(stmt)
            db_doc = result.scalar_one_or_none()
            
            return db_doc
        
        except SQLAlchemyError as e:
            logger.error(f"Database query error: {e}", exc_info=True)
            raise ValueError(f"Failed to retrieve the document") from e
        except Exception as e:
            logger.error(f"Unexpected query error: {e}", exc_info=True)
            raise
        
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
    
    def get_multi_by_user(
        self,
        db: Session,
        user_id: UUID,
        skip: int = 0,
        limit: int = 20
    ) -> Tuple[List[Document], int]:
        """
        分页查询用户文档列表
        """
        # 查询数据
        query = db.query(Document).filter(
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        )
        total = query.count()
        items = query.offset(skip).limit(limit).all()
        return list(items), total
    
    async def get_multi_by_user_async(
        self,
        db: AsyncSession,
        user_id: UUID,
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
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        ).offset(skip).limit(limit).order_by(Document.created_at.desc())  # 分页和排序
        result = await db.execute(stmt)
        
        # 返回文档列表：Sequence[Document] 类型
        items = result.scalars().all()  
        
        # 查询总数
        count_stmt = select(func.count()).where(
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        )
        total_result = await db.execute(count_stmt)
        total = total_result.scalar_one() 
    
        return list(items), total   
    
    async def get_multi_with_soft_deleted_async(
        self,
        db: AsyncSession,
        skip: int = 0,
        limit: int = 20,
    ) -> Tuple[List[Document], int]:
        """
        分页查询软删除的文档
        """
        # 查询数据
        stmt = select(Document).where(
            Document.is_deleted == True,
            Document.deleted_at.isnot(None)
        ).offset(skip).limit(limit).order_by(Document.created_at.desc())
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
        try:
            db_doc = db.query(Document).filter(
                Document.id == id,
                Document.user_id == user_id,
                Document.is_deleted == True,
                Document.deleted_at.isnot(None),
            ).first()
            
            return db_doc
        
        except SQLAlchemyError as e:
            logger.error(f"Database query error: id={id}, error={str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query error: id={id}, error={str(e)}", exc_info=True)
            raise
    
    async def get_soft_deleted_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: UUID,
    ):
        try:
            stmt = select(Document).where(
                Document.id == id,
                Document.user_id == user_id,
                Document.is_deleted == True,
                Document.deleted_at.isnot(None)
            )
            logger.debug(f"Executing get_soft_deleted_by_id_async query: id={id}, user_id={user_id}")
            result = await db.execute(stmt)
            db_doc = result.scalar_one_or_none()
            
            if db_doc is None:
                logger.info(f"Document not found in soft deleted state: id={id}")
                
            return db_doc
        
        except SQLAlchemyError as e:
            logger.error(f"Database query error: id={id}, error={str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query error: id={id}, error={str(e)}", exc_info=True)
            raise
        
    async def get_by_doc_id_async(
        self,
        db: AsyncSession,
        id: UUID, 
    ) -> Optional[Document]:
        try:
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
    
        except SQLAlchemyError as e:
            logger.error(f"Database query error: id={id}, error={str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query error: id={id}, error={str(e)}", exc_info=True)
            raise
    
    def create_record_with_user_id(
        self,
        db: Session,
        user_id: UUID,
        obj_in: DocumentCreate,
        storage_key: str,
        size_bytes: int,
        checksum: str,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
    ) -> Document:
        """
        创建文档并关联用户
        """
        try:
            db_doc = Document(
                filename=obj_in.filename,
                storage_key=storage_key,
                size_bytes=size_bytes,
                checksum=checksum,
                content_type=obj_in.content_type,
                storage_status=storage_status,
                processing_status=processing_status,
                user_id=user_id,
            )
            
            db.add(db_doc)
            db.commit()
            db.refresh(db_doc)
            return db_doc
        
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during create document: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error during create document: {str(e)}", exc_info=True)
            raise
    
    async def create_record_with_user_id_async(
        self,
        db: AsyncSession,
        user_id: UUID,
        obj_in: DocumentCreate,
        storage_key: str,
        size_bytes: int,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
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
            storage_status=storage_status,
            processing_status=processing_status,
            user_id=user_id,
        )
        
        db.add(db_doc)
        await db.commit()
        await db.refresh(db_doc)
        return db_doc
    
    def update_record_for_doc(
        self,
        db: Session,
        doc: Document,
        user_id: UUID,
        obj_in: DocumentCreate,
        storage_key: str,
        size_bytes: int,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
        checksum: str,
        error_message: Optional[str] = None,
    ) -> Document:
        try:
            doc.filename = obj_in.filename
            doc.storage_key = storage_key
            doc.size_bytes = size_bytes
            doc.checksum = checksum
            doc.content_type = obj_in.content_type if obj_in.content_type else doc.content_type
            doc.storage_status = storage_status
            doc.processing_status = processing_status
            doc.user_id = user_id
            if error_message:
                doc.error_message = error_message[:500]  # 截断防溢出
            db.commit()
            db.refresh(doc)
            return doc
        
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during update document {doc.id}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error during update document {doc.id}: {str(e)}", exc_info=True)
            raise
    
    def update_status(
        self,
        db: Session,
        doc: Document,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
        version_id: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> Document:
        """
        更新文档状态
        """
        try:
            doc.storage_status = storage_status
            doc.processing_status = processing_status
            doc.version_id = version_id
            if error_message:
                doc.error_message = error_message[:500]  # 截断防溢出
            db.commit()
            db.refresh(doc)
            return doc
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during update document {doc.id}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error during update document {doc.id}: {str(e)}", exc_info=True)
            raise
    
    async def update_status_async(
        self,
        db: AsyncSession,
        doc: Document,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
        error_message: Optional[str] = None
    ) -> Document:
        """
        更新文档状态
        """
        doc.storage_status = storage_status
        doc.processing_status = processing_status
        if error_message:
            doc.error_message = error_message[:500]  # 截断防溢出
        await db.commit()
        await db.refresh(doc)
        return doc
                
    async def update_status_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: UUID,
        storage_status: StorageStatus,
        processing_status: ProcessingStatus,
        error_message: Optional[str] = None
    ) -> bool:
        """
        直接通过 ID 更新文档状态（更高效，避免先 SELECT）
        """
        stmt = update(Document).where(
            Document.id == id,
            Document.user_id == user_id,
            Document.is_deleted == False,
            Document.deleted_at.is_(None)
        ).values(
            storage_status=storage_status,
            processing_status=processing_status,
            error_message=error_message[:500] if error_message else None  # 截断防溢出
        )
        
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount > 0
    
    def soft_delete(
        self,
        db: Session,
        doc: Document,
        deleted_at: datetime,
        updated_at: datetime,
        version_id: Optional[str],
    ):
        doc.storage_status = StorageStatus.DELETED
        doc.processing_status = ProcessingStatus.SUCCESS
        doc.is_deleted = True
        doc.deleted_at = deleted_at
        doc.updated_at = updated_at
        doc.version_id = version_id
        db.commit()
        db.refresh(doc)
        
        return doc

    async def soft_delete_with_return_async(
            self, 
            db: AsyncSession, 
            id: UUID, 
            user_id: UUID,
            deleted_at: datetime,
            updated_at: datetime,
            version_id: Optional[str],
        ) -> Optional[Document]:
        """
        软删除文档（标记删除时间）
        """
        try:
            stmt_update = update(Document).where(
                Document.id == id,
                Document.user_id == user_id,
                Document.is_deleted == False,
                Document.deleted_at.is_(None)
            ).values(
                is_deleted=True,
                deleted_at=deleted_at,
                updated_at=updated_at,
                version_id=version_id
            ).execution_options(synchronize_session="fetch")  # 确保能获取被更新的对象
            
            result = await db.execute(stmt_update)
            
            if result.rowcount == 0:
                await db.commit()
                return None
            
            stmt_select = select(Document).where(Document.id == id)
            result = await db.execute(stmt_select)
            db_doc = result.scalar_one_or_none() 
            
            await db.commit()
            return db_doc  # 返回删除后的最新状态
        
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during soft delete document {id}: {str(e)}")
            raise  # 向上抛出异常，让调用者处理
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during soft delete document {id}: {str(e)}")
            raise  # 向上抛出异常，让调用者处理
        
    
    async def soft_delete_by_id_async(
        self,
        db: AsyncSession,
        id: UUID,
        user_id: UUID,
        deleted_at: datetime,
        updated_at: datetime,
        version_id: Optional[str],
    ) -> bool:
        """软删除文档（直接通过 ID 更新）"""
        try:
            stmt = update(Document).where(
                Document.id == id,
                Document.user_id == user_id,
                Document.is_deleted == False,
                Document.deleted_at.is_(None)
            ).values(
                is_deleted=True,
                deleted_at=deleted_at,
                updated_at=updated_at,
                version_id=version_id
            ).execution_options(synchronize_session="fetch")  # 确保能获取被更新的对象
            
            result = await db.execute(stmt)
            await db.commit()
            
            deleted_count = result.rowcount
            if deleted_count == 0:
                logger.warning(f"Document may have been deleted or has no permissions, id: {id}")
            else:
                logger.info(f"Document successfully deleted, id: {id}")
            
            return deleted_count > 0
    
        except SQLAlchemyError as e:
            logger.error(f"Database error during soft delete document {id}: {str(e)}")
            await db.rollback()
            raise  # 向上抛出异常，让调用者处理
        except Exception as e:
            logger.error(f"Unexpected error during soft delete document {id}: {str(e)}")
            await db.rollback()
            raise  # 向上抛出异常，让调用者处理
        
    def restore(
        self,
        db: Session,
        doc: Document,
        deleted_at: Optional[datetime],
        updated_at: datetime,
        version_id: Optional[str],
    ):
        """恢复软删除的文档"""
        try:
            doc.storage_status = StorageStatus.ARCHIVED
            doc.processing_status = ProcessingStatus.SUCCESS
            doc.is_deleted = False
            doc.deleted_at = deleted_at
            doc.updated_at = updated_at
            doc.version_id = version_id
            db.commit()
            db.refresh(doc)
            
            return doc
        
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during restore document {doc.id}: {str(e)}")
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error during restore document {doc.id}: {str(e)}")
            raise
    
    def permanent_delete_by_id(
        self,
        db: Session,
        id: UUID,
        user_id: UUID,
    ):
        """永久删除文档记录"""
        try:
            stmt = delete(Document).where(
                Document.id == id,
                Document.user_id == user_id
            )
            db.execute(stmt)
            db.commit()
            return True
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during permanent delete document {id}: {str(e)}")
            db.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error during permanent delete document {id}: {str(e)}")
            db.rollback()
            raise
        
    def get_expired_soft_deleted(
        self,
        db: Session,
        cutoff_date: datetime,
        limit: int = 100,  # 添加批量限制
    ) -> List[Document]:
        """获取超过保留期限的软删除文档"""
        try:
            query = db.query(Document).filter(
                Document.is_deleted == True,
                Document.deleted_at.isnot(None),
                Document.deleted_at <= cutoff_date
            ).order_by(
                Document.deleted_at.asc()
            ).limit(limit)
            
            result = query.all()
            
            if not result:
                logger.info(f"No expired soft_deleted documents found before {cutoff_date.isoformat()}")
            
            return result
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during fetching expired soft_deleted documents: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during fetching expired soft_deleted documents: {str(e)}")
            raise


# 使用依赖注入   
async def get_document_dao():
    return DocumentCRUD()