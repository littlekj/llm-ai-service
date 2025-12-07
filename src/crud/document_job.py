import logging
from sqlalchemy.orm import Session
from sqlalchemy import select, func, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
from uuid import UUID

from src.models.document import Document
from src.models.document_job import DocumentJob, DocumentJobType, DocumentJobStatus

logger = logging.getLogger(__name__)


class DocumentJobCRUD:
    
    def create_document_job(
        self,
        db: Session,
        document_job: DocumentJob
    ) -> DocumentJob:
        db.add(document_job)
        db.flush()
        db.refresh(document_job)
        
        return document_job
        
    def get_document_job_by_type(
        self,
        db: Session,
        doc_id: UUID,
        job_type: DocumentJobType
    ):
        stmt = select(DocumentJob).where(
            DocumentJob.document_id == doc_id,
            DocumentJob.job_type == job_type,
        ).order_by(
            DocumentJob.created_at.desc()
        ).limit(1)
        
        result = db.execute(stmt)
        job = result.scalar_one_or_none()
        
        return job
    
    async def get_document_job_by_type_async(
        self,
        db: AsyncSession,
        doc_id: UUID,
        job_type: DocumentJobType
    ):
        stmt = select(DocumentJob).where(
            DocumentJob.document_id == doc_id,
            DocumentJob.job_type == job_type,
        ).order_by(
            DocumentJob.created_at.desc()
        ).limit(1)

        result = await db.execute(stmt)
        job = result.scalar_one_or_none()

        return job
    
    def get_document_job_by_id(
        self,
        db: Session,
        job_id: UUID,
    ):
        stmt = select(DocumentJob).where(
            DocumentJob.id == job_id,
        )
        
        result = db.execute(stmt)
        job = result.scalar_one_or_none()
        
        return job
        
    def get_document_jobs_by_doc_id(
        self,
        db: Session,
        doc_id: UUID,
    ):
        stmt = select(DocumentJob).where(
            DocumentJob.document_id == doc_id,
        ).order_by(
            DocumentJob.created_at.desc()
        )
        
        result = db.execute(stmt)
        jobs = result.scalars().all()
        
        return list(jobs)   
    
    async def get_document_jobs_by_doc_id_async(
        self,
        db: Session,
        doc_id: UUID,
        limit: int = 10,
        skip: int = 0
    ):
        stmt = select(DocumentJob).where(
            DocumentJob.document_id == doc_id,
        ).order_by(
            DocumentJob.created_at.desc()
        ).offset(skip).limit(limit)
        
        result = await db.execute(stmt)
        jobs = result.scalars().all()
        
        return jobs

    async def get_document_jobs_by_trace_id_async(
        self,
        db: AsyncSession,
        trace_id: str,
    ) -> List[DocumentJob]:
        stmt = (
            select(DocumentJob)
            .where(DocumentJob.trace_id == trace_id)
            .order_by(DocumentJob.created_at.asc())
        )
        result = await db.execute(stmt)
        return result.scalars().all()
    
    def delete_document_job(
        self,
        db: Session,
        doc_id: UUID,
    ) -> bool:
        stmt = delete(DocumentJob).where(
            DocumentJob.document_id == doc_id,
        )
        result = db.execute(stmt)
        return result.rowcount > 0
        
    def mark_running(
        self,
        db: Session,
        document_job: DocumentJob,
        job_type: DocumentJobType,
    ) -> DocumentJob:
        """标记任务开始"""
        document_job.job_type = job_type
        document_job.mark_running()
        db.flush()
        db.refresh(document_job)
        
        return document_job
    
    def mark_success(
        self,
        db: Session,
        document_job: DocumentJob,
        job_type: DocumentJobType,
        output_data: Optional[dict] = None,
    ) -> DocumentJob:
        """标记任务成功"""
        document_job.job_type = job_type
        document_job.mark_success(output_data)
        db.flush()
        db.refresh(document_job)
        
        return document_job
    
    def mark_failure(
        self, 
        db: Session, 
        document_job: DocumentJob, 
        job_type: DocumentJobType,
        error_message: str,
    ) -> DocumentJob:
        """标记任务失败"""
        document_job.job_type = job_type
        document_job.mark_failure(error_message)
        db.flush()
        db.refresh(document_job)
        
        return document_job
             
    def mark_retrying(
        self,
        db: Session,
        document_job: DocumentJob,
        job_type: DocumentJobType,
    ) -> DocumentJob:
        """标记任务重试"""        
        document_job.job_type = job_type
        document_job.mark_retrying()
        db.flush()
        db.refresh(document_job)

        return document_job
        
    def mark_timeout(
        self,
        db: Session,
        document_job: DocumentJob,
        job_type: DocumentJobType,
    ) -> DocumentJob:
        """标记任务超时"""
        document_job.job_type = job_type
        document_job.mark_timeout()
        db.flush()
        db.refresh(document_job)

        return document_job
        
    def is_terminated(
        self,
        db: Session,
        document_job: DocumentJob,
        job_type: DocumentJobType,
    ) -> bool:
        """任务是否终止"""
        stmt = select(DocumentJob).where(
            DocumentJob.id == document_job.id,
            DocumentJob.job_type == job_type
        )
        result = db.execute(stmt)
        db_document_job = result.scalar_one_or_none()

        return db_document_job.is_terminal()
            