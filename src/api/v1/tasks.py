from fastapi import APIRouter, HTTPException, Depends
from celery.result import AsyncResult
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import  datetime, timezone
import logging

from src.schemas.task import (
    TaskResultResponse,
    UploadTaskResult,
    DeleteTaskResult,
    RestoreTaskResult,
    PermanentDeleteTaskResult,
    DownloadTaskResult,
    ListObjectsResult,
    ScheduleDeletionResult
)
from src.workers.celery_app import celery_app
from src.core.depends import get_async_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])

TASK_SCHEMA_MAP = {
    "upload_document_task": UploadTaskResult,
    "soft_delete_document_task": DeleteTaskResult,
    "restore_document_task": RestoreTaskResult,
    "permanent_delete_document_task": PermanentDeleteTaskResult,
    "download_document_task": DownloadTaskResult,
    "list_objects_from_s3_task": ListObjectsResult,
    "schedule_permanent_deletion_task": ScheduleDeletionResult,
}

def _validate_task_result(task_name: str, result_data: dict) -> dict:
    """
    验证任务返回结果是否符合预期的 Pydantic 模型
    """
    schema_class = TASK_SCHEMA_MAP.get(task_name)
    if schema_class:
        try:
            validated_schema = schema_class(**result_data)
            return validated_schema.model_dump()
        except Exception as e:
            logger.warning(f"Task result validation failed for {task_name}: {str(e)}", extra={"result_data": result_data})
            # 如果验证失败，返回原始数据
            return result_data
    
    # 未知任务类型，返回原始数据
    return result_data

def _parse_task_result(result: AsyncResult) -> dict:
    """根据任务类型解析结果"""
    updated_at = result.date_done or  datetime.now(timezone.utc)
    
    # PENDING - 任务等待中
    if result.state == "PENDING":
        return {
            "task_id": result.id,
            "state": "PENDING",
            "progress": 0,
            "result": None,
            "error": None,
            "created_at": None,
            "updated_at": None
        }
    
    # STARTED - 任务已开始
    elif result.state == "STARTED":
        return {
            "task_id": result.id,
            "state": "STARTED",
            "progress": 10,
            "result": None,
            "error": None,
            "created_at": None,
            "updated_at": updated_at
        }
    
    # PROGRESS - 任务进行中
    elif result.state == "PROGRESS":
        meta = result.info if isinstance(result.info, dict) else {}
        return {
            "task_id": result.id,
            "state": "PROGRESS",
            "progress": meta.get("progress", 50),
            "result": None,
            "error": None,
            "created_at": None,
            "updated_at": updated_at
        }
    
    # SUCCESS - 任务成功
    elif result.state == "SUCCESS":
        # 直接返回任务的结果
        task_result = result.result if result.result else None
        if task_result and isinstance(task_result, dict):
            task_name = task_result.get("task_name")
            if task_name:
                task_result = _validate_task_result(task_name, task_result)
        return {
            "task_id": result.id,
            "state": "SUCCESS",
            "progress": 100,
            "result": task_result,  # 保持原始结构
            "error": None,
            "created_at": None,
            "updated_at": updated_at
        }
    
    # FAILURE - 任务失败
    elif result.state == "FAILURE":
        error_info = str(result.info) if result.info else "Unknown error"
        return {
            "task_id": result.id,
            "state": "FAILURE",
            "progress": 0,
            "result": None,
            "error": error_info,
            "created_at": None,
            "updated_at": updated_at
        }
    
    # RETRY - 任务重试中
    elif result.state == "RETRY":
        return {
            "task_id": result.id,
            "state": "RETRY",
            "progress": 0,
            "result": None,
            "error": "Task is being retried",
            "created_at": None,
            "updated_at": updated_at
        }
    
    # REVOKED - 任务已撤销
    elif result.state == "REVOKED":
        return {
            "task_id": result.id,
            "state": "REVOKED",
            "progress": 0,
            "result": None,
            "error": "Task has been revoked",
            "created_at": None,
            "updated_at": updated_at
        }
    
    # 其他未知状态
    else:
        return {
            "task_id": result.id,
            "state": result.state,
            "progress": 0,
            "result": None,
            "error": None,
            "created_at": None,
            "updated_at": updated_at
        }
    

@router.get("/{task_id}", response_model=TaskResultResponse)
async def get_task_result(task_id: str) -> TaskResultResponse:
    """
    查询任务执行结果
    
    支持的任务类型:
    - upload_document_task: 上传文档
    - soft_delete_document_task: 软删除文档
    - restore_document_task: 恢复文档
    - permanent_delete_document_task: 永久删除文档
    - download_document_task: 下载文档
    - list_objects_from_s3_task: 列出对象
    - schedule_permanent_deletion_task: 调度删除任务
    - ... 其他任务类型可扩展
    """
    try:
        result = AsyncResult(task_id, app=celery_app)
        response_data = _parse_task_result(result)
        
        return TaskResultResponse(**response_data)
    
    except Exception as e:
        logger.exception(f"Error retrieving task result for {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve task result")
    
@router.delete("/{task_id}")
async def revoke_task(task_id: str) -> dict:
    """
    撤销正在执行的任务
    """
    try:
        celery_app.control.revoke(task_id, terminate=True)
        logger.info(f"Task {task_id} has been revoked")
        return {
            "status": "success",
            "message": f"Task {task_id} has been revoked"
        }
    except Exception as e:
        logger.exception(f"Error revoking task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to revoke task")