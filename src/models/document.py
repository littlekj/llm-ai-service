from __future__ import annotations  # 开启延迟解析，避免循环导入问题
import uuid
from sqlalchemy import Integer, String, Boolean, func
from sqlalchemy import BigInteger, Text, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime, timezone
from enum import Enum
from src.models import Base

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .user import User
    

class DocumentStatus(str, Enum):
        UPLOADED = "uploaded"
        PROCESSING = "processing"
        INDEXED = "indexed"
        FAILED = "failed"


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
        server_default=text("gen_random_uuid()")  # PostgreSQL 函数
    )
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)  # 对象存储路径
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)  # BIGINT 支持大文件    
    content_type: Mapped[str] = mapped_column(String(100), nullable=True)
        
    status: Mapped[str] = mapped_column(
        String(20), 
        default=DocumentStatus.UPLOADED, 
        server_default=text(f"'{DocumentStatus.UPLOADED.value}'"), 
        nullable=False,
    )  # uploaded/processing/indexed/failed

    checksum: Mapped[str] = mapped_column(String(128), nullable=True)  # 文件校验和
    error_message: Mapped[str] = mapped_column(Text, nullable=True)  # 处理失败原因

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),  # 更新时自动更新；高合规业务使用数据库触发器 + ORM onupdate
        nullable=False,
    )
    
    # is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)  # 软删除标志
    deleted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )  # 软删除时间
    
    # 外键：指向 users.id
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # 多对一关系：Document -> User
    user: Mapped["User"] = relationship("User", back_populates="documents")
    
    # 命名规范：ix_<表名>_<字段1>_<字段2>
    __table_args__ = (
        Index("ix_documents_user_status", "user_id", "status"),  # 用户ID和状态联合索引
        Index("ix_documents_created_at", "created_at"),  # 创建时间索引
        Index("ix_documents_deleted_at", "deleted_at"),  # 软删除时间索引
    )
