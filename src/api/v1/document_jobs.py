from fastapi import APIRouter, Depends
from fastapi import Query
from fastapi import Request, HTTPException
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import logging

from src.models.user import User
from src.models.document_job import DocumentJobType, DocumentJobStatus
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.crud.document import DocumentCRUD
from src.crud.document_job import DocumentJobCRUD
from src.workers.document import vector_storage
from src.middleware.request_id import request_id_ctx_var


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/document_jobs", tags=["document_jobs"])

@router.post("/vectorize/{doc_id}")
async def vectorize_document(
    doc_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
):
    """
    向量化文档（需要优先完成文本提取）
    """
    logger.info(f"Vectorizing document {doc_id} for user {current_user.id}")
    
    request_id = request_id_ctx_var.get()    
    
    # 验证文档归属
    documen_crud = DocumentCRUD()
    doc = await documen_crud.get_by_id_async(db, doc_id, current_user.id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    # 获取文档处理任务
    document_job_crud = DocumentJobCRUD()
    job = await document_job_crud.get_document_job_by_type_async(
        db=db,
        doc_id=doc_id, 
        job_type=DocumentJobType.EMBED_CHUNKS
    )
    logger.info(f"Checking job status: job={job}, job_type={job.job_type if job else None}, status={job.status if job else None}")

    # 检查文档是否已经向量化
    if job and job.job_type == DocumentJobType.EMBED_CHUNKS and job.status == DocumentJobStatus.SUCCESS.value:
        logger.warning(f"Document has been vectorized")
        raise HTTPException(status_code=400, detail="Document has been vectorized")  
    else:
        # 提交向量化任务
        result = vector_storage.process_document_task.apply_async(
            args=[{
                "document": {
                    "id": str(doc.id),
                    "user_id": str(current_user.id),
                },
                "document_job": {
                    "id": None,
                    "stage_order": 0,
                }      
            }],
            headers={"request_id": request_id} if request_id else None,
        )

        logger.info(f"Document vertorization task submitted")
        return {
            "task_id": result.id,
            "status": result.status,
            "message": "Document vectorization task submitted"
        }
        
@router.get("/{doc_id}/jobs")
async def get_document_jobs(
    doc_id: UUID, 
    db: AsyncSession = Depends(get_async_session), 
    current_user: User = Depends(get_current_user),
    limit: int = Query(20, ge=1, le=100, description="返回记录数量限制"),
    skip: int = Query(0, ge=0, description="跳过记录数量"),
):
    """查询文档的所有处理任务（业务视角）"""
    
    logger.info(f"Getting document jobs for document {doc_id} for user {current_user.id}")

    document_job_crud = DocumentJobCRUD()
    jobs = await document_job_crud.get_document_jobs_by_doc_id_async(
        db=db,
        doc_id=doc_id,
        limit=limit,
        skip=skip,
    )
    
    job_list = []
    for job in jobs:
        job_info = {
            "job_id": str(job.id),
            "job_type": job.job_type.value,
            "status": job.status,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "execution_time": job.get_execution_time(),
            "error_message": job.error_message,
        }
        
        job_list.append(job_info)
        
    return {"doc_id": doc_id, "jobs": job_list, "limit": limit, "skip": skip}


@router.get("/trace/{trace_id}")
async def get_trace_jobs(
    trace_id: str,
    db: AsyncSession = Depends(get_async_session),
):
    """
    通过 trace_id 查询整个请求链路的所有任务
    
    用于分布式追踪和故障排查
    """
    document_job_crud = DocumentJobCRUD()
    jobs = await document_job_crud.get_document_jobs_by_trace_id_async(
        db=db,
        trace_id=trace_id,
    )
    
    if not jobs:
        raise HTTPException(status_code=404, detail="No jobs found for trace_id")
    
    # 统计信息
    total = len(jobs)
    success = sum(1 for j in jobs if j.status == DocumentJobStatus.SUCCESS.value)
    failure = sum(1 for j in jobs if j.status == DocumentJobStatus.FAILURE.value)
    running = sum(1 for j in jobs if j.status == DocumentJobStatus.RUNNING.value)
    
    return {
        "trace_id": trace_id,
        "summary": {
            "total": total,
            "success": success,
            "failure": failure,
            "running": running,
        },
        "jobs": [
            {
                "job_id": str(job.id),
                "job_type": job.job_type.value,
                "status": job.status,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "execution_time": job.get_execution_time(),
            }
            for job in jobs
        ]
    }
