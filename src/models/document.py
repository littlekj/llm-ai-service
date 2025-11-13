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
from typing import TYPE_CHECKING, Optional, Dict, Any

if TYPE_CHECKING:
    from .user import User


# class DocumentStatus(str, Enum):
#     """文档的业务处理状态枚举类"""
#     UPLOADED = "uploaded"
#     PROCESSING = "processing"
#     INDEXED = "indexed"
#     FAILED = "failed"

class StorageStatus(str, Enum):
    """文档的存储状态枚举类"""
    ACTIVE = "active"
    DELETED = "deleted"
    ARCHIVED = "archived"


class ProcessingStatus(str, Enum):
    """文档的处理状态枚举类"""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class Document(Base):
    """
    Document类，用于表示系统中的文档实体
    对应于数据库中的documents表
    """
    __tablename__ = "documents"

    # 使用 UUID 主键（PostgreSQL）
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        server_default=text("gen_random_uuid()"),  # PostgreSQL 函数
        nullable=False,
    )
    
    # 文档文件名
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # 存储路径，用于访问对象存储
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False, unique=True) 
    
    # 文件大小
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)  # BIGINT 支持大文件
    
    # 文件的MIME类型
    content_type: Mapped[str] = mapped_column(String(100), nullable=True)
    
    # 文件扩展名（用于查询优化）
    file_extension: Mapped[str] = mapped_column(String(20), nullable=True, index=True)

    # 校验和，用于文件完整性验证
    checksum: Mapped[str] = mapped_column(String(128), nullable=True)
    
    # 存储状态
    storage_status: Mapped[str] = mapped_column(
        String,
        default=StorageStatus.ACTIVE.value,
        server_default=StorageStatus.ACTIVE.value, 
        nullable=False,
    )
    
    # 处理状态
    processing_status: Mapped[str] = mapped_column(
        String,
        default=ProcessingStatus.PENDING.value,  # 初始状态
        server_default=ProcessingStatus.PENDING.value,
        nullable=False,
    )
    
    # 错误信息，用于处理失败时的记录
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    
    # 文档的元数据，使用 JSONB 类型存储
    doc_metadata: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=True,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    # 创建时间
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    
    # 更新时间，`onupdate=func.now()` 确保每次更新时自动更新
    # 高合规业务使用数据库触发器 + ORM onupdate
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),  
        nullable=False,
    )

    # 文档处理完成时间
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
        index=True,
    )

    # 软删除标记
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
   
    # 软删除时间
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    ) 

    # 关联的任务ID（用于追踪任务处理）
    trace_id: Mapped[Optional[str]] = mapped_column(
        String(64), index=True, nullable=True,
        comment="分布式追踪ID，用于日志链路分析"
    )
    
    # 对象存储版本ID
    version_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True,
        comment="对象存储中的版本ID"
    )
    
    # 外键：指向 users.id
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # 多对一关系：Document -> User
    user: Mapped["User"] = relationship("User", back_populates="documents")
    
    # 关联任务（一对多）
    jobs: Mapped[list["DocumentJob"]] = relationship(
        "DocumentJob",
        back_populates="document",
        cascade="all, delete-orphan",
        lazy="selectin"  # 推荐用于列表加载
    )
    
    # 索引优化：多字段联合索引，提升查询效率
    # 命名规范：ix_<表名>_<字段1>_<字段2>
    __table_args__ = (
        Index("ix_documents_user_storage_status", "user_id", "storage_status"), 
        Index("ix_documents_processing_status", "processing_status"),
        Index("ix_documents_processed_at", "processed_at"),
        Index("ix_documents_trace_id", "trace_id"),
        Index("ix_documents_file_extension", "file_extension"),
        Index(
            "ix_documents_storage_key_prefix","storage_key", 
            postgresql_ops={"storage_key": "text_pattern_ops"}
        ),
        Index("ix_documents_user_status_created", "user_id", "storage_status", "created_at"),
        # {"schema": "public"}  # 指定数据库模式
    )
    
    # 业务方法
    def soft_delete(self, deleted_at: Optional[datetime] = None) -> None:
        """软删除操作，更新文档状态为已删除"""
        self.storage_status = StorageStatus.DELETED
        self.is_deleted = True
        self.deleted_at = deleted_at or datetime.now(timezone.utc)

    def restore(self) -> None:
        """恢复文档，将其状态恢复为活动"""
        self.storage_status = StorageStatus.ACTIVE
        self.is_deleted = False
        self.deleted_at = None

    def mark_processing(self) -> None:
        """标记文档为处理中"""
        self.processing_status = ProcessingStatus.PROCESSING

    def mark_success(self, processed_at: Optional[datetime] = None) -> None:
        """标记文档处理成功"""
        self.processing_status = ProcessingStatus.SUCCESS
        now = datetime.now(timezone.utc)
        self.processed_at = processed_at or now

    def mark_failure(self, error_message: str) -> None:
        """标记文档处理失败"""
        self.processing_status = ProcessingStatus.FAILURE
        self.error_message = error_message
        
    def __repr__(self) -> str:
        return f"<Document(id={self.id}, filename='{self.filename}', status={self.storage_status})>"
