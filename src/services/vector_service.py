import logging
import asyncio
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from typing import Optional, List, Dict, Any, Callable
from uuid import UUID
from pathlib import Path
from minio.error import S3Error, ServerError, InvalidResponseError
from requests.exceptions import ConnectionError, Timeout, RequestException

from src.models.document import Document
from src.models.document_job import DocumentJob, DocumentJobType, DocumentJobStatus
from src.utils.text_splitter import TextSplitter
from src.utils.qdrant_storage import QdrantClient
from src.utils.minio_storage import MinioClient
from src.utils.extract_text import extract_text_by_type
from src.crud.document_job import DocumentJobCRUD
from src.crud.vector_chunk import VectorChunkCRUD
from src.core.exceptions import (
    BusinessLogicError,
    NotFoundError,
    ResourceConflictError,
    DatabaseError,
    ExternalServiceError,
)

logger = logging.getLogger(__name__)

class VectorizationService:
    """
    封装文档处理、分块、向量化、存储到向量库的流程
    """
    def __init__(self):
        self.text_splitter = TextSplitter()
        self.vector_store = QdrantClient()
        self.minio_client = MinioClient()
        
    def _execute_job(
        self,
        db: Session,
        doc_id: UUID,
        user_id: UUID,
        job_type: DocumentJobType,
        parent_job_id: Optional[UUID],
        context: Dict[str, Any],
        logic_fn: Callable
    ) -> tuple[Dict[str, Any], UUID]:
        try:
            document_job_crud = DocumentJobCRUD()
            existing_job = document_job_crud.get_document_job_by_type(
                db=db,
                doc_id=doc_id,
                job_type=job_type,
            )

            validate_job = None
            
            if existing_job: 
                if existing_job.is_terminal():
                    if existing_job.status == DocumentJobStatus.SUCCESS.value:
                        # 如果已有任务且成功，则直接返回结果
                        if job_type == DocumentJobType.EMBED_CHUNKS:
                            return {
                                "job_id": str(existing_job.id),
                                "job_type": job_type.value,
                                "status": existing_job.status,
                            }, validate_job.id
            
                    # 如果任务已终止但未成功，创建新任务
                    validate_job = DocumentJob(
                        document_id=doc_id,
                        user_id=user_id,
                        job_type=job_type,
                        status=DocumentJobStatus.PENDING.value,
                        parent_job_id=parent_job_id,
                        retry_of_job_id=existing_job.id,
                        **context,
                    )
                    validate_job = document_job_crud.create_document_job(db, validate_job)
                elif existing_job.status == DocumentJobStatus.RETRYING.value:
                    # 如果已有任务且正在重试，则使用已有任务
                    validate_job = existing_job
                else:
                    # 其他任务状态不允许重试
                    raise BusinessLogicError(message="The current state job cannot be retried")  
            else:
                validate_job = DocumentJob(
                    document_id=doc_id,
                    user_id=user_id,
                    job_type=job_type,
                    status=DocumentJobStatus.PENDING.value,
                    parent_job_id=parent_job_id,
                    retry_of_job_id=existing_job.id if existing_job else None,
                    **context,
                )
                validate_job = document_job_crud.create_document_job(db, validate_job)
                
            document_job_crud.mark_running(db, validate_job, job_type)
            
            result = logic_fn()
            
            document_job_crud.mark_success(db, validate_job, job_type, result["summary"])
            db.commit()
            
            logger.info(f"{job_type.value} job completed successfully for document {doc_id}.")
            
            return result["result"], validate_job.id
            
        except BusinessLogicError as e:
            # 业务错误，不需要重试
            db.rollback()
            logger.error(f"{job_type.value} job failed: {str(e)}", exc_info=True)
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message=str(e),
                    )
                    db.commit()
                except Exception:
                    pass
            raise
        
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error during {job_type.value} job: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message="Database integrity error occurred.",
                    )
                    db.commit()
                except Exception:
                    pass
            raise ResourceConflictError(
                message="Database integrity error occurred during the job.",
                error_code="database_integrity_error",
            )  from e
            
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error during {job_type.value} job: {str(e)}", exc_info=True)
            
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message="Database error occurred.",
                    )
                    db.commit()
                except Exception:
                    pass
            raise DatabaseError(
                message="Database error occurred during the job.",
                error_code="database_error",
            ) from e
        except (ConnectionError, Timeout, RequestException, S3Error) as e:
            # 可重试异常
            db.rollback()
            logger.warning(f"{job_type.value} job failed with retryable error: {str(e)}", exc_info=True)
            if validate_job:
                try:
                    document_job_crud.mark_retrying(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                    )
                    db.commit()
                except Exception:
                    pass
            raise ExternalServiceError(
                service_name=f"{job_type.value} related service",
                message="Temporary external service error occurred during the job.",
            ) from e
        except Exception as e:
            # 不可重试：业务错误/代码异常
            db.rollback()
            logger.error(f"{job_type.value} job unexpectedly failed: {str(e)}", exc_info=True)
            if validate_job:
                try:
                    document_job_crud.mark_failure(
                        db=db,
                        document_job=validate_job,
                        job_type=job_type,
                        error_message="Unexpected error occurred during job execution.",
                    )
                    db.commit()
                except Exception:
                    pass
            raise
    
    def process_document_pipeline(
        self,
        db: Session,
        doc_id: UUID,
        user_id: UUID,
        task_id: str,
        parent_job_id: Optional[UUID],
        chain_id: str,
        trace_id: str,
        stage_order: int,
    ) -> Dict[str, Any]:
        """
        文档向量化处理流程：文本提取 - 分块 - 向量嵌入 - 存储到向量库

        :param db: 数据库会话
        :param doc_id: 文档ID
        :param task_id: 任务ID
        :param parent_job_id: 上游任务ID
        :param chain_id: 链ID
        :param trace_id: 跟踪ID
        :return: 处理结果
        """
        document = db.query(Document).filter(Document.id == doc_id).first()
        if not document:
            raise NotFoundError(resource="Document", resource_id=str(doc_id))
        
        context = {"task_id": task_id, "chain_id": chain_id, "trace_id": trace_id}
        current_parent_job_id = parent_job_id
        current_stage = stage_order + 1
        
        # 文本提取
        text_content, current_job_id = self._execute_job(
            db=db,
            doc_id=doc_id,
            user_id=user_id,
            job_type=DocumentJobType.EXTRACT_TEXT,
            parent_job_id=current_parent_job_id,
            context={**context, "stage_order": current_stage},
            logic_fn=lambda: self._extract_text_content(document)
                
        )
        # text_content, current_parent_job_id = self._extract_text_content(
        #     db, document, task_id, current_parent_job_id, chain_id, trace_id, current_stage
        # )
        current_stage += 1
        
        # 文本分块
        chunks, current_job_id = self._execute_job(
            db=db,
            doc_id=doc_id,
            user_id=user_id,
            job_type=DocumentJobType.CHUNK_TEXT,
            parent_job_id=current_job_id,
            context={**context, "stage_order": current_stage},
            logic_fn=lambda: self._chunk_text_content(document, text_content)
        )
        # chunks, current_parent_job_id = self._chunk_text_content(
        #     db, document, text_content, task_id, current_parent_job_id, chain_id, trace_id, current_stage
        # )
        current_stage += 1
        
        # 向量嵌入和存储
        upsert_result, validate_job_id = self._execute_job(
            db=db,
            doc_id=doc_id,
            user_id=user_id,
            job_type=DocumentJobType.EMBED_CHUNKS,
            parent_job_id=current_job_id,
            context={**context, "stage_order": current_stage},
            logic_fn=lambda: self._embed_chunks_and_store(db, user_id, document, chunks)
        )
        # upsert_result, _ = self._embed_chunks_and_store(
        #     db, user_id, document, chunks, task_id, parent_job_id, chain_id, trace_id, current_stage
        # )
        
        logger.info(f"Successfully completed vectorization pipeline for document {doc_id}.")
        
        return {
            "vector_db_result": upsert_result,
            "vector_db_job_id": str(validate_job_id),
        } 
    
    def _extract_text_content(
        self,
        doc: Document,
    ) -> Dict[str, Any]:
        try:
            # 从 MinIO 获取文件
            response = self.minio_client.get_object(doc.storage_key)
            file_ext = Path(doc.filename).suffix.lower()
            text_content = extract_text_by_type(
                response=response,
                file_ext=file_ext,
                filename=doc.filename,
            )
            logger.info(f"Document downloaded from MinIO: {doc.id}")
        finally:
            # 检查 response 变量在当前作用域中是否存在
            if "response" in locals() and hasattr(response, "close"):
                response.close()
        
        # 验证文本内容
        if not text_content or not text_content.strip():
            raise NotFoundError(resource="Document", resource_id=str(doc.id))
                
        return {
            "result": text_content,
            "summary": "Text extraction completed successfully.",
        }
            
    def _chunk_text_content(
        self,
        doc: Document,
        text: str,
    ): 
        chunks = self.text_splitter.split_text(
            text=text,
            metadata={
                "document_id": str(doc.id),
                "filename": doc.filename,
                # "user_id": str(user_id),
            }
        )
        
        if not chunks:
            raise NotFoundError(resource="Text chunks")
        
        logger.info(f"Text chunk job completed successfully: {doc.id}")
        
        return {
            "result": chunks,
            "summary": "Text chunk job completed successfully",
        }
    
    def _embed_chunks_and_store(
        self,
        db: Session,
        user_id: UUID,
        doc: Document,
        chunks: List[str],
    ):
        # 向量化并存储到 Qdrant
        if not self.vector_store:
            raise RuntimeError("Vector store is not configured")
 
        # 在同步方法中调用异步方法
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            vector_result = loop.run_until_complete(
                self.vector_store.add_chunks(
                    chunks=chunks,
                    document_id=doc.id,
                    user_id=user_id,
                )
            )
        finally:
            loop.close()
            
        logger.info(f"Vectors added to Qdrant: {vector_result.get('added_count', 0)} points")
        
        # 记录元数据到数据库
        chunks_data = [
            {
                "point_id": point.id,
                "content": point.payload["content"],
                "chunk_index": point.payload["metadata"]["chunk_index"],
                "page_number": point.payload["metadata"].get("page_number"),
            }
            for point in vector_result.get("points", [])
        ]
        
        if not chunks_data:
            raise ExternalServiceError(
                service_name="Qdrant",
                message="Vector store returned no points after embedding",
                details={
                    "input_chunks_count": len(chunks), 
                    "document_id": str(doc.id)
                }
            )
        
        vector_chunk_crud = VectorChunkCRUD()
        db_chunks = vector_chunk_crud.create_chunks_batch(
            db=db,
            document_id=doc.id,
            user_id=user_id,
            chunks_data=chunks_data,
        )
        
        logger.info(f"Document processed and stored in vector store: {len(db_chunks)} chunks")
        
        return {
            "result": {
                "document_id": str(doc.id),
                "chunks_count": len(db_chunks),
            },
            "summary": "Document processed and stored in vector store",
        }
