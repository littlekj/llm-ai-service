from fastapi import APIRouter
from celery.result import AsyncResult
import logging

from src.schemas.task import TaskResultResponse
from src.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])

@router.get("/tasks/{task_id}", response_model=TaskResultResponse)
async def get_task_result(task_id: str):
    result = AsyncResult(task_id, app=celery_app)
    
    if result.state == "PENDING":
        return {
            "task_id": task_id,
            "state": "pending",
            "progress": 0
        }
    elif result.state == "SUCCESS":
        return {
            "task_id": task_id,
            "state": "success",
            "progress": 100,
            "result": result.result  # 任务实际返回的数据结果
        }
    elif result.state == "FAILURE":
        return {
            "task_id": task_id,
            "state": "failure",
            "progress": 0,
            "error": str(result.info)  # 任务失败时的异常信息或进度信息
        }
    else:
        return {
            "task_id": task_id,
            "state": result.state,
            "progress": 50
        }