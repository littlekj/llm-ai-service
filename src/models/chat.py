import uuid
from sqlalchemy import Column, String, Integer, ForeignKey, Text, DateTime, Boolean
from sqlalchemy import func, text, Index
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column,relationship
from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Optional, TYPE_CHECKING

from src.models.base import Base


if TYPE_CHECKING:
    from src.models.user import User
    
class MessageRole(str, PyEnum):
    """消息角色枚举类"""
    USER = "user"
    ASSISTANT = "assistant"

class ChatSession(Base):
    """存储会话数据模型类"""
    __tablename__ = "chat_sessions"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        primary_key=True, 
        server_default=func.gen_random_uuid()
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        ForeignKey("users.id", ondelete="CASCADE"), 
        nullable=True
    )
    client_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False
    )  # 客户端唯一标识
    title: Mapped[str] = mapped_column(String(255), nullable=False)
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
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    
    # token 使用统计
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    
    # 关系
    messages: Mapped[list["ChatMessage"]] = relationship(
        "ChatMessage", 
        back_populates="session",
        cascade="all, delete-orphan",  # 级联删除
    )
    user: Mapped["User"] = relationship(
        "User", 
        back_populates="chat_sessions"
    )
    
    def mark_as_deleted(self):
        """标记为已删除"""
        self.is_deleted = True
        self.deleted_at = datetime.now(timezone.utc)

class ChatMessage(Base):
    """存储消息数据模型类"""
    __tablename__ = "chat_messages"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        primary_key=True, 
        server_default=text("gen_random_uuid()")
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        ForeignKey("chat_sessions.id", ondelete="CASCADE"), 
        nullable=False
    )
    # client_id: Mapped[uuid.UUID] = mapped_column(
    #     UUID(as_uuid=True),
    #     nullable=False,
    # )
    role: Mapped[str] = mapped_column(
        String, 
        default=MessageRole.USER.value,
        server_default=MessageRole.USER.value,
        nullable=False
    )  # user/assistant
    content: Mapped[str] = mapped_column(Text, nullable=False)
    used_tokens: Mapped[int] = mapped_column(Integer, default=0)
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
    
    # 关系
    session: Mapped["ChatSession"] = relationship(
        "ChatSession", back_populates="messages"
    )
    
class ChatCall(Base):
    """存储每次 LLM 调用的详细信息"""
    __tablename__ = "chat_calls"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        primary_key=True,
        server_default=func.gen_random_uuid()
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        ForeignKey("chat_sessions.id", ondelete="CASCADE"),
        nullable=False
    )
    # client_id: Mapped[uuid.UUID] = mapped_column(
    #     UUID(as_uuid=True),
    #     nullable=False
    # )
    # token 统计
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    
    # 性能统计
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    
    # 关系
    session: Mapped["ChatSession"] = relationship("ChatSession", backref="calls")
    
# 所有索引集中定义，便于维护        
ChatSession.__table_args__ = (
    Index("ix_chat_sessions_user_id", "user_id"),
    Index("ix_chat_sessions_client_id", "client_id"),
    Index("ix_chat_sessions_user_deleted", "user_id", "is_deleted"),
    Index("ix_chat_sessions_is_deleted", "is_deleted"),
)

ChatMessage.__table_args__ = (
    Index("ix_chat_messages_session_id", "session_id"),
    Index("ix_chat_messages_role", "role"),
    Index("ix_chat_messages_created_at", "created_at"),
)

ChatCall.__table_args__ = (
    Index("ix_chat_calls_session_id", "session_id"),
    Index("ix_chat_calls_created_at", "created_at"),
)