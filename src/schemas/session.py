from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from uuid import UUID
from typing import Optional, Generic, TypeVar


T = TypeVar('T')

class SessionResponse(BaseModel):
    """会话响应模型"""
    id: UUID
    user_id: Optional[UUID]
    client_id: UUID
    title: str  # 通常是第一个问题
    created_at: datetime
    updated_at: datetime
    message_count: int
    
class ChatMessageSchema(BaseModel):
    """消息响应模型"""
    id: UUID
    session_id: UUID
    role: str  # user or assistant
    content: str
    created_at: datetime
    used_tokens: int
    
class ChatMessagePaginatedResponse(BaseModel, Generic[T]):
    """会话消息分页响应模型"""
    items: List[T] = Field(default_factory=list)
    total: int = Field(..., ge=0, description="总记录数")
    page: int = Field(..., ge=0, description="当前页码")
    size: int = Field(..., ge=0, description="每页记录数")
    pages: int = Field(..., ge=0, description="总页数")
    
# class SessionHistoryResponse(BaseModel):
#     """历史会话响应模型"""
#     id: UUID
#     title: str
#     messages: List[ChatMessageSchema]
#     created_at: datetime
#     updated_at: datetime
    
class ChatResponse(BaseModel):
    content: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0