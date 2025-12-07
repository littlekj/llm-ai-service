from __future__ import annotations  # 开启延迟解析
from sqlalchemy.orm import Mapped, mapped_column, relationship, backref
from sqlalchemy import (
    Integer, String, Text, Boolean, func, text, ForeignKey, Index
)
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, JSONB
from sqlalchemy import Enum as SA_Enum
from enum import Enum
from datetime import datetime, timezone
from typing import Optional, Dict, Any, TYPE_CHECKING

from src.models import Base
import uuid

if TYPE_CHECKING:
    from src.models.user import User
    from src.models.document import Document


class DocumentJobType(str, Enum):
    """文档处理任务类型"""
    # 对象操作
    UPLOAD_DOCUMENT = "upload_document"
    
    # 文件验证
    VALIDATE_FILE = "validate_file"
    
    # 文本提取
    EXTRACT_TEXT = "extract_text"
    PARSE_PDF = "parse_pdf"
    OCR = "ocr"
    
    # 文本处理
    CHUNK_TEXT = "chunk_text"
    
    # 向量化
    EMBED_CHUNKS = "embed_chunks"
    
    # 其他处理
    CLASSIFY_CONTENT = "classify_content"
    GENERATE_PREVIEW = "generate_preview"
    CONVERT_TO_WEB = "convert_to_web"

    
class DocumentJobStatus(str, Enum):
    """文档处理任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRYING = "retrying"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class DocumentJob(Base):
    """
    文档处理任务模型（管理文档处理的各个阶段）
    
    职责：
    - 任务类型和状态
    - 任务执行时间（开始、结束）
    - 重试逻辑
    - 错误信息
    - 输入输出数据
    
    设计思路：
    - 一个文档多个任务（extract_text, embed_document 等）
    - 每个任务独立管理其生命周期
    - 支持任务依赖，（通过 triggered_by 字段）
    """
    __tablename__ = "document_jobs"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    
    # ======= 关联文档 =======
    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    document: Mapped["Document"] = relationship("Document", back_populates="jobs")
    
    # ======= 任务信息 =======
    job_type: Mapped[DocumentJobType] = mapped_column(
        SA_Enum(
            DocumentJobType, 
            name="document_job_type_enum", 
            create_type=True,
            # 使用枚举的 .value (小写字符串) 作为数据库存储和查找的依据
            values_callable=lambda obj: [e.value for e in obj]
        ),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        default=DocumentJobStatus.PENDING.value,
        server_default=text(f"'{DocumentJobStatus.PENDING.value}'"),
        nullable=False,
        index=True,
    )
    
    # ======= 追踪信息 =======
    trace_id: Mapped[Optional[str]] = mapped_column(
        String(64), 
        nullable=True, 
        index=True,
        comment="请求追踪 ID（关联一次用户操作的所有任务）"
    )
    task_id: Mapped[Optional[str]] = mapped_column(
        String(64), 
        nullable=True, 
        index=True,
        comment="Celery 任务 ID（用户查询实时状态）"
    )
    chain_id: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        index=True,
        comment="Celery 任务链 ID（chain.apply_async()返回的主任务 ID）"
    )
    stage_order: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text("0"),
        nullable=False,
        comment="任务在处理流程中的顺序（例如：0=上传, 1=提取, 2=向量化...）"
    )

    # ======= 任务依赖 =======
    parent_job_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("document_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="任务链中的父任务 ID（业务依赖关系）"
    )
    
    retry_of_job_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("document_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="重试任务的原始任务 ID（重试逻辑）"
    )

    # ======= 任务执行时间 =======
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
        index=True,
    )
    finished_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), 
        nullable=True,
        index=True,
    )
    
    # ======= 输入输出数据 =======
    input_data: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=True,
        default=dict,
        server_default=text("'{}'::jsonb")
    )
    output_data: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=True,
        default=dict,
        server_default=text("'{}'::jsonb")
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # ======= 时间戳 =======
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    
    # 用户信息（冗余字段，用于快速查询）
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="触发任务的用户 ID（冗余字段，避免 JOIN documents 表）"
    )
    user: Mapped[Optional["User"]] = relationship("User", back_populates="jobs")
    
    parent_job: Mapped[Optional["DocumentJob"]] = relationship(
        "DocumentJob",
        remote_side=[id],  # 指向自身主键
        back_populates="children_jobs",  # 与children_jobs互相关联
        foreign_keys=[parent_job_id]  # 使用parent_job_id作为外键
    )
    children_jobs: Mapped[list["DocumentJob"]] = relationship(
        "DocumentJob",
        back_populates="parent_job",  # 与parent_job互相关联
        cascade="all, delete-orphan",  # 级联删除子任务
        foreign_keys=[parent_job_id]  # 使用同一个外键
    )

    retry_of_job: Mapped[Optional["DocumentJob"]] = relationship(
        "DocumentJob",
        remote_side=[id],
        backref=backref("retried_by_jobs"),  # 自动创建反向关系
        foreign_keys=[retry_of_job_id]
    )

    __table_args__ = (
        Index("ix_document_jobs_doc_type", "document_id", "job_type"),
        Index("ix_document_jobs_status_created", "status", "created_at"),
        Index("ix_document_jobs_type_status", "job_type", "status"),
        Index("ix_document_jobs_trace_id", "trace_id"),
        Index("ix_document_jobs_task_id", "task_id"),
        # 唯一约束：同一文档的同一类型任务只能有一个处于 RUNNING 状态
        Index(
            "ix_document_jobs_unique_running",
            "document_id", "job_type", "status",
            unique=True,
            postgresql_where=text(f"status = '{DocumentJobStatus.RUNNING.value}'"),
        ),
        # {"schema": "public"}  # 指定表所在的schema
    )
    
    # ======= 业务方法 =======
    def mark_running(self) -> None:
        """标记任务为运行中"""
        self.status = DocumentJobStatus.RUNNING.value
        self.started_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        
    def mark_success(self, output: Optional[Dict[str, Any]] = None) -> None:
        """标记任务为成功"""
        self.status = DocumentJobStatus.SUCCESS.value
        if output:
            self.output_data = output
        self.error_message = None
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_failure(self, error: Optional[str]) -> None:
        """标记任务为失败"""
        self.status = DocumentJobStatus.FAILURE.value
        self.error_message = error[:512]
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
            
    def mark_retrying(self) -> None:
        """标记任务为重试中"""
        self.status = DocumentJobStatus.RETRYING.value
        # self.attempt_count += 1
        self.updated_at = datetime.now(timezone.utc)
        
    def mark_timeout(self) -> None:
        """标记任务为超时"""
        self.status = DocumentJobStatus.TIMEOUT.value
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def is_terminal(self) -> bool:
        """检查任务是否处于最终状态"""
        return self.status in (
            DocumentJobStatus.SUCCESS.value,
            DocumentJobStatus.CANCELLED.value,
            DocumentJobStatus.TIMEOUT.value,
            DocumentJobStatus.FAILURE.value,
        )
        
    def get_execution_time(self) -> Optional[float]:
        """获取任务的执行时间（秒）"""
        if not self.started_at:
            return None
        end_time = self.finished_at or datetime.now(timezone.utc)
        return (end_time - self.started_at).total_seconds()
    
    def __repr__(self) -> str:
        return (
            f"<DocumentJob("
            f"id={self.id}, "
            f"doc_id={self.document_id}, "
            f"type={self.job_type.value}, "
            f"status={self.status})>"
        )