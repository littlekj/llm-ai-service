
from pydantic import BaseModel, Field
from typing import Optional

from src.schemas.document import DocumentResponse


class FileDetail(DocumentResponse):
    pass


class TaskResult(BaseModel):
    result: FileDetail = Field(..., description="任务结果")
    

class TaskResultResponse(BaseModel):
    
    task_id: str = Field(..., description="任务 ID")
    state: str = Field(..., description="任务状态")
    progress: int = Field(..., description="任务进度")
    result: Optional[TaskResult] = Field(None, description="任务结果")
    error: Optional[str] = Field(None, description="任务错误信息")
    
    
    