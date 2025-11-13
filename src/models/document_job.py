from __future__ import annotations  # 开启延迟解析
from sqlalchemy.orm import Mapped, mapped_column, relationship
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
    from src.models import Document


class DocumentJobType(str, Enum):
    PARSE_PDF = "parse_pdf"
    OCR = "ocr"
    EXTRACT_TEXT = "extract_text"
    EMEBED_DOCUMENT = "embed_document"
    CLASSIFY_CONTENT = "classify_content"
    GENERATE_PREVIEW = "generate_preview"
    CONVERT_TO_WEB = "convert_to_web"
    VALEDATE_FILE = "validate_file"
    CHUNK_TEXT = "chunk_text"
    
class DocumentJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRYING = "retrying"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class DocumentJob(Base):
    __tablename__ = "document_jobs"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    
    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    document: Mapped["Document"] = relationship("Document", back_populates="jobs")
    
    job_type: Mapped[DocumentJobType] = mapped_column(
        SA_Enum(DocumentJobType, name="document_job_type_enum"),
        nullable=False,
        index=True,
    )
    
    status: Mapped[str] = mapped_column(
        String,
        default=DocumentJobStatus.PENDING.value,
        server_default=DocumentJobStatus.PENDING.value,
        nullable=False,
        index=True,
    )
    
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    retry_delay_secs: Mapped[int] = mapped_column(Integer, default=30)

    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    timeout_after_secs: Mapped[int] = mapped_column(Integer, default=600)  # 10 minutes
    
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
    trace_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    triggered_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        onupdate=func.now(),
    )
    
    # 是否幂等（用于重试决策）
    is_idempotent: Mapped[bool] = mapped_column(Boolean, default=True)
    
    __table_args__ = (
        Index("ix_document_jobs_status_created", "status", created_at),
        Index("ix_document_jobs_type_status", "job_type", "status"),
        Index("ix_document_jobs_trace_id", "trace_id"),
        Index("ix_document_jobs_task_id", "task_id"),
        Index("ix_document_jobs_started_at", "started_at"),
        Index("ix_document_jobs_finished_at", "finished_at"),
        # {"schema": "public"}  # 指定表所在的schema
    )
    
    def mark_running(self) -> None:
        self.status = DocumentJobStatus.RUNNING
        self.started_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        
    def mark_success(self, output: Optional[Dict[str, Any]] = None) -> None:
        self.status = DocumentJobStatus.SUCCESS
        if output:
            self.output_data = output
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_failure(self, error: str, increment_attempt: bool = True) -> None:
        self.status = DocumentJobStatus.FAILURE
        self.error_message = error
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        if increment_attempt:
            self.attempt_count += 1
            
    def mark_retrying(self) -> None:
        self.status = DocumentJobStatus.RETRYING
        self.attempt_count += 1
        self.updated_at = datetime.now(timezone.utc)
        
    def is_terminal(self) -> bool:
        return self.status in (
            DocumentJobStatus.SUCCESS,
            DocumentJobStatus.CANCELLED,
            DocumentJobStatus.TIMEOUT
        )
        
    def is_retryable(self) -> bool:
        return (
            self.status in (DocumentJobStatus.FAILURE, DocumentJobStatus.RETRYING)
            and self.attempt_count < self.max_retries
        )
        
    def is_timed_out(self) -> bool:
        if not self.started_at or not self.timeout_after_secs:
            return False
        now = datetime.now(timezone.utc)
        return (now - self.started_at).total_seconds() > self.timeout_after_secs
    
    def __repr__(self) -> str:
        return (
            f"<DocumentJob(id={self.id}, "
            f"doc_id={self.document_id}, "
            f"job_type={self.job_type}, "
            f"status={self.status}, "
            f"attempts={self.attempt_count}/{self.max_retries})>"
        )