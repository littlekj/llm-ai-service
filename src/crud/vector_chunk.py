import logging
from typing import Optional, List
from sqlalchemy import select, delete
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from src.models.vector_chunk import VectorChunk


logger = logging.getLogger(__name__)

class VectorChunkCRUD:
    """向量切片 CRUD 操作"""
    
    def create_chunks_batch(
        self,
        db: Session,
        document_id: UUID,
        user_id: UUID,
        chunks_data: List[dict],
    ) -> List[VectorChunk]:
        """
        批量创建向量切片
        
        :param db: 数据库会话
        :param document_id: 文档 ID
        :param user_id: 用户 ID
        :param chunks_data: 向量切片数据（包含 point_id, content, chunk_index 等）
        :return: 创建的向量切片列表
        """
        chunks = []
        for chunk_data in chunks_data:
            chunk = VectorChunk(
                document_id=document_id,
                user_id=user_id,
                point_id=chunk_data["point_id"],
                content=chunk_data["content"][:50],  # 截断内容，防止过长
                chunk_index=chunk_data["chunk_index"],
                page_number=chunk_data.get("page_number"),
            )
            # db.add(chunk)
            chunks.append(chunk)
        
        db.add_all(chunks)    
        db.flush()
        
        return chunks
        
    def get_chunks_by_doc_id(
        self,
        db: AsyncSession,
        document_id: UUID,
        user_id: UUID,
    ) -> List[VectorChunk]:
        """
        根据文档 ID 获取向量切片
        """
        query = select(VectorChunk).where(
            VectorChunk.document_id == document_id,
            VectorChunk.user_id == user_id
        )
        result = db.execute(query)
        
        return result.scalars().all()
    
    def delete_chunks_by_doc_id(
        self,
        db: Session,
        document_id: UUID,
        user_id: UUID,
    ) -> int:
        """
        删除文档的所有向量切片
        """
        query = delete(VectorChunk).where(
            VectorChunk.document_id == document_id,
            VectorChunk.user_id == user_id
        )
        result = db.execute(query)
        db.flush()
        
        return result.rowcount
            