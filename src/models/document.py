from __future__ import annotations  # 开启延迟解析，避免循环导入问题
import uuid
from sqlalchemy import Integer, String, Boolean, func
from sqlalchemy import BigInteger, Text, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Enum as SA_Enum
from datetime import datetime, timezone
from enum import Enum
from src.models import Base
from src.models.document_job import DocumentJob
from typing import TYPE_CHECKING, Optional, Dict, Any, List

if TYPE_CHECKING:
    from src.models.user import User
    from src.models.document_job import DocumentJob
    from src.models.vector_chunk import VectorChunk


# ======= 存储状态 =======
class StorageStatus(str, Enum):
    """文档的对象存储状态枚举类"""
    UPLOADING = "uploading"     # 上传中
    ACTIVE = "active"           # 已上传，可用
    DELETED = "deleted"         # 已删除
    ARCHIVED = "archived"       # 已归档
    CORRUPTED = "corrupted"     # 已损坏

class Document(Base):
    """
    文档元数据模型（只管理文档本身的属性）
    
    职责：
    - 文件信息（名称、类型、大小）
    - 存储位置（S3/MinIO key）
    - 所属用户
    """
    __tablename__ = "documents"

    # 使用 UUID 主键（PostgreSQL）
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),  # PostgreSQL 函数
        nullable=False,
    )
    
    # ======= 所有权 =======
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )  # 外键：指向 users.id，级联删除
    
    # ======= 文档基本信息 =======
    filename: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    file_extension: Mapped[str] = mapped_column(String(20), nullable=True, index=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=True)  # BIGINT 支持大文件
    content_type: Mapped[str] = mapped_column(String(100), nullable=True)
    checksum: Mapped[str] = mapped_column(String(128), nullable=True)  # 文件完整性校验码
    
    # ======= 存储信息 =======
    storage_key: Mapped[Optional[str]] = mapped_column(
        String(512), 
        unique=True,    # 保证有值时不重复
        nullable=True,  # 允许为空（允许多个 NULL）
        comment="对象存储中的唯一键（路径）"
    )
    storage_status: Mapped[str] = mapped_column(
        String,
        default=StorageStatus.UPLOADING.value,
        server_default=StorageStatus.UPLOADING.value, 
        nullable=False,
        comment="存储状态：上传中、已激活、已删除、已归档、已损坏等",
        index=True,
    )  # 存储状态（对象存储）
    version_id: Mapped[Optional[str]] = mapped_column(
        String(64), 
        nullable=True,
        comment="对象存储中的版本ID"
    )  # 对象存储版本ID
    
    # ======= 元数据 =======
    doc_metadata: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
        server_default=text("'{}'::jsonb"),
        nullable=True,
    )
    
    # ======= 时间戳 =======
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    # 高合规业务使用数据库触发器 + ORM onupdate 
    # `onupdate=func.now()` 确保每次更新时自动更新
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),  
        nullable=False,
    )
    
    # ======= 软删除 =======
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)  # 软删除标记
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
        index=True,
    )  # 软删除时间
    
    # 多对一关系：Document -> User
    user: Mapped["User"] = relationship("User", back_populates="documents")
    
    # 一对多关系：关联任务
    jobs: Mapped[List["DocumentJob"]] = relationship(
        "DocumentJob",
        back_populates="document",
        cascade="all, delete-orphan",
        lazy="dynamic"  # 延迟加载，支持查询
    )
    # 一对多关系：关联向量块
    vector_chunks: Mapped[List["VectorChunk"]] = relationship(
        "VectorChunk",
        back_populates="document",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    
    # 索引优化：多字段联合索引，提升查询效率
    # 命名规范：ix_<表名>_<字段1>_<字段2>
    __table_args__ = (
        Index("ix_documents_user_created", "user_id", "created_at"),
        Index("ix_documents_user_deleted", "user_id", "deleted_at"),
        Index("ix_documents_filename", "filename"),
        # {"schema": "public"}  # 指定数据库模式
    )
        
    def __repr__(self) -> str:
        return (
            f"<Document(id={self.id}, filename='{self.filename}', user_id={self.user_id})>"
        )