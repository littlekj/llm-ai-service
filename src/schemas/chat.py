from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime

class QuestionRequest(BaseModel):
    """问答请求模型"""
    question: str = Field(..., min_length=1, max_length=2000, description="问题")
    document_ids: Optional[List[UUID]] = None
    session_id: Optional[UUID] = None
    
class SourceReference(BaseModel):
    """引用源信息模型"""
    document_id: UUID
    document_name: str
    content_snippet: str
    page_number: Optional[int] = None
    
class QuestionResponse(BaseModel):
    """问答响应模型"""
    answer: str
    sources: List[SourceReference]
    tokens_used: int
    latency_ms: int
    session_id: Optional[UUID] = None
    
    