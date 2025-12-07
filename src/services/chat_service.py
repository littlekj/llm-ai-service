import time
import json
import re
import httpx
import logging
from fastapi import Depends
from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from uuid import UUID, uuid4

from src.utils.llm_client import LLMClient
from src.utils.qdrant_storage import QdrantClient
from src.models.user import User
from src.schemas.chat import QuestionResponse
from src.crud.chat import ChatCRUD
from src.crud.document import DocumentCRUD
from src.crud.user import UserCRUD
from src.middleware.request_id import request_id_ctx_var
from src.schemas.chat import SourceReference
from src.utils.prompts import PromptTemplates
from src.core.exceptions import (
    PermissionDeniedError,
    DatabaseError,
    VectorStoreError,
    ExternalServiceError,
)


logger = logging.getLogger(__name__)

class ChatService:
    def __init__(
        self,
        llm_client: LLMClient,
        vector_store: QdrantClient,
        chat_crud: ChatCRUD,
        user_crud: UserCRUD,
    ):
        self.llm_client = llm_client
        self.vector_store = vector_store
        self.chat_crud = chat_crud
        self.user_crud = user_crud

    async def get_answer(
        self,
        db: AsyncSession,
        user: Optional[User],
        question: str,
        document_ids: Optional[List[UUID]],
        session_id: Optional[UUID],
        client_id: UUID,   
    ) -> QuestionResponse:
        """获取问题答案"""
        start_time = time.time()
        request_id = request_id_ctx_var.get()
        logger.info(f"Processing question: {question[:100]}...")
        
        try:
            # 获取或创建会话
            session = None
            if session_id:
                # 获取会话
                session = await self.chat_crud.get_session_by_id_async(
                    db=db,
                    session_id=session_id,
                    client_id=client_id,
                    user_id=user.id if user else None
                )
                # # 游客模式：如果找到会话但属于其他用户，应创建新会话
                # if session and user is None and session.user_id is not None:
                #     session = None
                #     session_id = None  # 强制创建新会话
                
            if not session:
                # 创建会话
                session = await self.chat_crud.create_session_async(
                    db=db,
                    client_id=client_id,
                    id=session_id if session_id else uuid4(),
                    obj_in={
                        "user_id": user.id if user else None,
                        "title": question[:100],
                    }
                )
                
            context = ""
            sources = []  # 初始化为空列表
            retrieved_chunks = []  # 保存检索到的文档块用于记录
            
            # 检测查询意图
            intent = self._detect_query_intent(question)
            logger.info(f"Detected query intent: {intent}")

            if intent == "summarize":
                # 摘要模式
                min_score = 0.0
                limit = 10
                logger.info(f"Using summarize mode: min_score={min_score}, limit={limit}")
            else:
                # 具体模式
                min_score = 0.3
                limit = 5
                logger.info(f"Using specific mode: min_score={min_score}, limit={limit}")
                
            # 向量搜索
            if document_ids:
                # 向量检索相关文档片段
                retrieved_chunks = await self.vector_store.similarity_search(
                    query=question,
                    filter_doc_ids=document_ids,
                    limit=limit,
                    min_score=min_score,
                )
            
                # 构建上下文和引用源
                context = self._build_context(retrieved_chunks)
                sources = await self._build_sources(db, retrieved_chunks)
                
            # 构建提示
            messages = self._build_prompt(
                question=question,
                context=context,
                history=await self._get_recent_history(
                    db, session.id, session.client_id, session.user_id
                ) if session else None
            )

            # 调用 LLM 获取答案
            llm_response = await self.llm_client.chat_completion(messages=messages)

            # 解析 LLM 响应
            # 尝试将模型返回解析为 JSON（遵循我们在模板中要求的结构）
            final_answer, final_sources = self._parse_llm_response(
                llm_response=llm_response,
                sources=sources,
            )
            
            # 保存对话记录（保留原始模型返回内容）
            await self.chat_crud.create_message_async(
                db,
                session_id=session.id,
                role="user",
                content=question,
                used_tokens=llm_response.prompt_tokens,
            )
            await self.chat_crud.create_message_async(
                db,
                session_id=session.id,
                role="assistant", 
                content=llm_response.content,
                used_tokens=llm_response.completion_tokens,
            )
            
            # # 记录检索到的文档块（用于后续分析）
            # if retrieved_chunks:
            #     for chunk in retrieved_chunks:
            #         await self._record_chunk_retrieval(
            #             db=db,
            #             session_id=session.id,
            #             point_id=chunk["point_id"],
            #             similarity_score=chunk["score"],
            #         )
                    
            # 计算延迟和更新统计
            latency = int((time.time() -start_time) * 1000)
            
            # 保存 LLM 调用记录
            await self.chat_crud.create_call_record_async(
                db=db,
                session_id=session.id,
                prompt_tokens=llm_response.prompt_tokens,
                completion_tokens=llm_response.completion_tokens,
                total_tokens=llm_response.total_tokens,
                latency_ms=latency,
            )
            
            # 更新会话 token 使用量
            await self.chat_crud.update_session_token_stats_async(
                db=db,
                session_id=session.id,
                prompt_tokens=llm_response.prompt_tokens,
                completion_tokens=llm_response.completion_tokens,
                total_tokens=llm_response.total_tokens,
            )
            
            # Token 配额管理
            if user:
                # 登录用户：检查并更新配额
                result = await self.user_crud.update_token_stats_async(
                    db=db,
                    user_id=user.id,
                    used_tokens=llm_response.total_tokens,
                )
                if not result:
                    logger.warning(f"Insufficient quota for user {user.id}, rolling back transaction.")
                    await db.rollback()
                    raise PermissionDeniedError(message="Insufficient quota")
                
            else:
                # 游客：可以设置默认限制或记录使用量
                await self._handle_guest_token_usage(
                    db=db,
                    client_id=client_id,
                    used_tokens=llm_response.total_tokens
                )

            # 提交事务
            await db.commit()

            # 返回答案
            return QuestionResponse(
                answer=final_answer,
                sources=final_sources,
                tokens_used=llm_response.total_tokens,
                latency_ms=latency,
                session_id=session.id,
            )
            
        except IntegrityError as e:
            await db.rollback()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            raise DatabaseError(message="Database integrity error") from e
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise DatabaseError(message="Database error") from e
        except VectorStoreError as e:
            await db.rollback()
            logger.error(f"Vector store error: {str(e)}", exc_info=True)
            raise
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            await db.rollback()
            logger.error(f"LLM service error: {str(e)}", exc_info=True)
            raise ExternalServiceError(
                service_name="LLMService", message="LLM service error"
            ) from e
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error in get_answer: {str(e)}", exc_info=True)
            raise 
        
    def _detect_query_intent(self, query: str) -> str:
        """
        检测查询意图
        
        :param query: 用户输入的查询
        :return: 查询意图，"summarize" 或 "specific"
        """
        summarize_keywords = [
            "总结", "概括", "归纳", "汇总", "介绍",
            "summarize", "summary", "overview", "introduce"
        ]
        
        query_lower = query.lower()

        for keyword in summarize_keywords:
            if keyword in query_lower:
                return "summarize"

        return "specific"
    
    def _parse_llm_response(self, llm_response, sources):
        """解析 LLM 响应"""
        raw_content = llm_response.content.strip() if llm_response.content else ""
        parsed = None
        final_answer = ""
        final_sources = [] # 默认为空，等待筛选
        try:
            parsed = json.loads(raw_content)
        except Exception:
            # 尝试从返回文本中抽取第一个 JSON 对象
            m = re.search(r'(\{.*\})', raw_content, re.S)
            if m:
                try:
                    parsed = json.loads(m.group(1))
                except Exception:
                    parsed = None

        if parsed and isinstance(parsed, dict) and parsed.get("answer"):
            # 将 answer 强制为字符串，防止类型为 None 或非字符串
            try:
                final_answer = str(parsed.get("answer") or "")
            except Exception:
                final_answer = ""
                
            # 如果模型返回了 sources 字段并且格式合理，则使用它；否则使用检索到的 sources
            s = parsed.get("sources")
            final_sources = s if (isinstance(s, list) and s) else list(sources)
        else:
            # 无法解析为结构化 JSON，退回使用原始文本作为 answer，并保留检索到的 sources
            final_answer = raw_content
            final_sources = list(sources)
            
        return final_answer, final_sources
        
        
    def _build_context(self, chunks: List[Dict[str, Any]]) -> str:
        """构建上下文信息"""
        if not chunks:
            return ""
        
        context_parts = []
        for i, chunk in enumerate(chunks):
            content = chunk.get("content", "").strip()
            if content:
                # 给每个片段加一个 ID，例如 [1], [2]
                context_parts.append(f"--- 文档片段 [{i + 1}] --- \n{content}\n")
        
        return "\n\n".join(context_parts)
    
    def _build_prompt(
        self,
        question: str,
        context: str,
        history: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, str]]:
        """构建 LLM 提示

        使用 `PromptTemplates.format_chat_context` 生成统一的 system 提示和上下文，
        并在模板末尾追加 JSON 输出约束，要求模型只输出 JSON（便于后端解析）。
        返回一个 messages 列表，最后一项为用户问题（role=user）。
        """
        # 基础模板：优先使用文档问答模板，如果未来需要可按场景切换
        template = PromptTemplates.get_document_qa_prompt()

        # 在模板末尾强制输出为 JSON，以便后端解析
        json_constraint = (
            "\n\n严格的输出格式要求：\n"
            "1. 必须且只能输出一个 JSON 对象\n"
            "2. JSON 对象格式为: {\"answer\": \"答案内容\", \"sources\": []}\n"
            "3. 禁止在 JSON 前后添加任何其他文本（包括 ```json 标记）\n"
            "4. answer 必须是字符串类型\n"
            "5. sources 必须是数组类型（没有引用源时使用空数组）\n"
            "示例：\n"
            "{\"answer\": \"这是一个示例答案\", \"sources\": []}\n\n"
        )
        template = template + json_constraint
        logger.debug(f"Final prompt template: {template}")

        # format_chat_context 会返回 system（template）和可选的 context/history
        messages = PromptTemplates.format_chat_context(template, context=context, history=history)

        # 最后附上用户问题
        messages.append({"role": "user", "content": question})
        logger.debug(f"Final messages: {messages}")

        return messages
    
    async def _get_recent_history(
        self,
        db: AsyncSession,
        session_id: UUID,
        client_id: UUID,
        user_id: Optional[UUID],
        limit: int = 5,
    ):
        """获取最近的对话历史"""
        messages = await self.chat_crud.get_recent_messages_async(
            db=db,
            session_id=session_id,
            client_id=client_id,
            user_id=user_id,
            limit=limit
        )
        
        return [
            {
                "role": msg.role,
                "content": msg.content,
            } for msg in messages
        ] if messages else []
           
    async def _build_sources(
        self,
        db: AsyncSession,  # 异步数据库会话对象，用于数据库操作
        chunks: List[Dict[str, Any]],
    ) -> List[SourceReference]:
        """构建引用源信息（按文档去重聚合）源信息"""
        if not chunks:
            return []

        # 提取去重后的 document_id 集合
        doc_ids = {
            str(chunk.get("metadata", {}).get("document_id"))
            for chunk in chunks 
            if chunk.get("metadata", {}).get("document_id")
        }
                
        if not doc_ids:
            return []
                
        document_crud = DocumentCRUD()
        
        # 批量查询数据库（或者遍历去重后的ID查询），建立映射表
        docs_map = {}
        for doc_id in doc_ids:
            doc = await document_crud.get_by_doc_id_async(db, id=UUID(doc_id))
            if doc:
                docs_map[doc_id] = doc
        
        # 使用字典来辅助去重，Key 为文档 ID
        unique_sources_map = {}
        
        for chunk in chunks:
            metadata = chunk.get("metadata", {})
            doc_id_str = str(metadata.get("document_id"))
            
            if doc_id_str in docs_map:
                if doc_id_str in unique_sources_map:
                    continue  # 已存在则跳过
                
                doc = docs_map[doc_id_str]
                
                unique_sources_map[doc_id_str] = {
                    "document_id": doc.id,
                    "document_name": doc.filename,
                    "content_snippet": chunk["content"][:100], # 取第一个匹配片段的摘要
                    "page_number": metadata.get("page_number"),
                }
        
        return list(unique_sources_map.values())
    
    async def _handle_guest_token_usage(
        self,
        db: AsyncSession,
        client_id: UUID,
        used_tokens: int
    ):
        """处理游客的token使用量"""
        # 可以实现：
        # 1. 记录游客使用量
        # 2. 设置每日/每小时限制
        # 3. 使用Redis等缓存系统实现限流
        pass
        