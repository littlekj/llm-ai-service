
from pydantic import BaseModel, Field
from typing import Optional, Literal, List, Dict, Any
from datetime import datetime, timezone


class TaskStatusEnum:
    """任务状态枚举"""
    PENDING = "PENDING"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"
 
class BaseTaskResult(BaseModel):
    """基础任务结果模型"""
    status: str = Field(..., description="当前阶段状态")
    task_id: str = Field(..., description="任务 ID")
    message: Optional[str] = Field(None, description="附加消息")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat(), description="完成时间")
    
class DocumentInfo(BaseModel):
    """文档基本信息"""
    doc_id: str = Field(..., description="文档 ID")
    user_id: str = Field(..., description="用户 ID")
    filename: str = Field(..., description="文件名")
    size_bytes: int = Field(..., description="文件大小（字节）")
    content_type: str = Field(..., description="内容类型")

    
class UploadTaskResult(BaseTaskResult):
    """上传任务结果"""
    document: DocumentInfo
    storage_status: str = Field(..., description="存储状态")
    version_id: Optional[str] = Field(None, description="版本 ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    
class DeleteTaskResult(BaseTaskResult):
    """删除任务结果"""
    document: DocumentInfo
    version_id: Optional[str] = Field(None, description="版本 ID")
    is_deleted: bool = Field(..., description="是否已删除")
    deleted_at: Optional[str] = Field(None, description="删除时间（UTC）")
    

class RestoreTaskResult(BaseTaskResult):
    """恢复任务结果"""
    document: DocumentInfo
    storage_status: str = Field(..., description="存储状态")
    version_id: Optional[str] = Field(None, description="版本 ID")
    restored_at: str = Field(..., description="恢复时间（UTC）")
    
class ObjectDetail(BaseModel):
    """对象详情"""
    obj_name: str = Field(..., description="对象名称")
    last_modified: str = Field(..., description="最后修改时间")
    etag: str = Field(..., description="ETag 标识")
    size: int = Field(..., description="对象大小（字节）")
    metadata: dict = Field(..., description="对象元数据")
    version_id: Optional[str] = Field(None, description="版本 ID")
    is_latest: Optional[bool] = Field(None, description="是否为最新版本")
    is_delete_marker: bool = Field(..., description="是否为删除标记")
    
class ListObjectsResult(BaseTaskResult):
    """列出对象任务结果"""
    objects: List[ObjectDetail] = Field(default_factory=list, description="对象列表")

class PermanentDeleteTaskResult(BaseTaskResult):
    """永久删除任务结果"""
    doc_id: str = Field(..., description="文档 ID")
    filename: Optional[str] = Field(None, description="文件名")
    
class DownloadTaskResult(BaseTaskResult):
    """下载任务结果"""
    content: bytes = Field(..., description="文件内容的字节流")
    filename: str = Field(..., description="文件名")
    size_bytes: int = Field(..., description="文件大小（字节）")
    content_type: str = Field(..., description="内容类型")
    
class BatchOperationResult(BaseTaskResult):
    """批量操作结果"""
    total: int = Field(..., description="总操作数")
    succeeded: int = Field(..., description="成功操作数")
    failed: int = Field(..., description="失败操作数")
    results: List[dict] = Field(default_factory=list, description="每个操作的结果详情")
    
class ScheduleDeletionResult(BaseTaskResult):
    """调度删除任务结果"""
    scheduled_count: int = Field(..., description="已调度数量")
    expired_docs: List[dict] = Field(default_factory=list, description="过期文档列表")
    

class TaskResultResponse(BaseModel):
    """任务查询响应"""
    task_id: str = Field(..., description="任务 ID")
    state: str = Field(..., description="任务状态", examples=["SUCCESS"])
    progress: int = Field(default=0, ge=0, le=100, description="任务进度")
    result: Optional[Any] = Field(None, description="任务结果（根据任务类型不同）")
    error: Optional[str] = Field(None, description="任务错误信息")

    # TODO: 使用数据库记录任务的创建和更新时间
    created_at: Optional[datetime] = Field(None, description="任务创建时间（UTC）")
    updated_at: Optional[datetime] = Field(None, description="任务最后更新时间（UTC）")
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "abc123def456",
                "state": "SUCCESS",
                "progress": 100,
                "result": {
                     "status": "success",
                     "task_id": "abc123def456",
                     "message": "Document uploaded successfully",
                     "timestamp": "2025-11-11T10:00:00Z",
                },
                "error": None,
                "created_at": "2025-11-11T10:00:00Z",
                "updated_at": "2025-11-11T10:05:00Z"
            }
        }

