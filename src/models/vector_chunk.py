from sqlalchemy import String, Integer, Float, Text, ForeignKey, Index, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
from datetime import datetime
from typing import TYPE_CHECKING
import uuid

from src.models.base import Base

if TYPE_CHECKING:
    from src.models.document import Document
    from src.models.user import User

class VectorChunk(Base):
    """向量切片元数据表"""
    __tablename__ = "vector_chunks"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    
    # Qdrant 中 point ID
    point_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    
    # 块的内容
    content: Mapped[str] = mapped_column(Text, nullable=False)
    
    # 块的索引
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # 块在文档中的位置（可选）
    page_number: Mapped[int] = mapped_column(Integer, nullable=True)
    
    # 向量维度
    embedding_dim: Mapped[int] = mapped_column(Integer, default=384)
    
    # 相似度分数（用于缓存）
    similarity_score: Mapped[float] = mapped_column(Float, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    
    # 关联的文档
    document: Mapped["Document"] = relationship("Document", back_populates="vector_chunks")
    # 关联的用户
    user: Mapped["User"] = relationship("User", back_populates="vector_chunks")
    
    __table_args__ = (
        Index('idx_doc_user', 'document_id', 'user_id'),
        Index('idx_point_id', 'point_id'),
        Index('idx_created_at', 'created_at'),
    )
    