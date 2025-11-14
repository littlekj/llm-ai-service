import time
import json
import re
import logging
from fastapi import Depends
from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from uuid import UUID, uuid4

from src.utils.llm_client import LLMClient
from src.utils.qdrant_storage import QdrantClient
from src.models.user import User
from src.schemas.chat import QuestionResponse
from src.crud.chat import ChatCRUD
from src.crud.document import DocumentCRUD, get_document_dao
from src.crud.user import UserCRUD
from src.middleware.request_id import request_id_ctx_var
from src.schemas.chat import SourceReference
from src.utils.prompts import PromptTemplates


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
                
            # elif not session.user_id and user:
            #     # 如果会话不存在,但用户存在,则将会话与用户关联
            #     await self.chat_crud.attach_session_to_user_async(
            #         db=db,
            #         session_id=session.id,
            #         user_id=user.id
            #     )
                
            context = ""
            sources = []  # 初始化为空列表
            if document_ids:
                # 向量检索相关文档片断
                relevant_chunks = await self.vector_store.similarity_search(
                    query=question,
                    filter_doc_ids=document_ids,
                    limit=5,
                )
            
                # 构建上下文和引用源
                context = self._build_context(relevant_chunks)
                sources = await self._build_sources(db, relevant_chunks)
                
            # 构建提示
            messages = self._build_prompt(
                question=question,
                context=context,
                history=await self._get_recent_history(
                    db, session.id, session.client_id, session.user_id
                ) if session else None
            )
            # logger.info(f"Prompt: {messages}")
            # 调用 LLM 获取答案
            llm_response = await self.llm_client.chat_completion(messages=messages)
            logger.info(f"LLM response: {llm_response.content[:100]}...")
            # 尝试将模型返回解析为 JSON（遵循我们在模板中要求的结构）
            raw_content = llm_response.content.strip() if llm_response.content else ""
            parsed = None
            # 默认回退值
            final_answer = ""
            final_sources = sources
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
                final_sources = s if isinstance(s, list) else list(sources)
            else:
                # 无法解析为结构化 JSON，退回使用原始文本作为 answer，并保留检索到的 sources
                final_answer = raw_content
                final_sources = list(sources)
            
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
            
            # 计算延迟
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
            
            # 更新用户 token 使用量
            if user:
                await self.user_crud.update_token_stats_async(
                    db=db,
                    user_id=user.id,
                    used_tokens=llm_response.total_tokens,
                )

            # 返回答案
            return QuestionResponse(
                answer=final_answer,
                sources=final_sources,
                tokens_used=llm_response.total_tokens,
                latency_ms=latency,
                session_id=session.id,
            )
        
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}", exc_info=True)
            raise
        
        
    def _build_context(self, chunks: List[Dict[str, Any]]) -> str:
        """构建上下文信息"""
        if not chunks:
            return ""
        
        context_parts = []
        for i, chunk in enumerate(chunks):
            content = chunk.get("content", "").strip()
            if content:
                context_parts.append(f"文档片断 {i + 1}:\n{content}")
        
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

        # format_chat_context 会返回 system（template）和可选的 context/history
        messages = PromptTemplates.format_chat_context(template, context=context, history=history)

        # 最后附上用户问题
        messages.append({"role": "user", "content": question})

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
        db: AsyncSession,
        chunks: List[Dict[str, Any]],
        document_crud: DocumentCRUD = Depends(get_document_dao),
    ) -> List[SourceReference]:
        """构建引用源信息"""
        sources = []
        for chunk in chunks:
            metadata = chunk.get("metadata", {})
            doc_id = metadata.get("document_id")
        
            try:
               if doc_id:
                doc = await document_crud.get_by_doc_id_async(db, id=doc_id)
                if doc:
                    sources.append({
                        "document_id": doc.id,
                        "document_name": doc.filename,
                        "current_snippet": chunk["content"][:200],
                        "page_number": metadata.get("page_number"),
                    })
            
            except SQLAlchemyError as e:
                logger.error(f"Database query error for doc_id={str(doc_id)}: error={str(e)}", exc_info=True)
                raise
            except Exception as e:
               logger.error(f"Error building source reference for doc_id={str(doc_id)}: {str(e)}", exc_info=True)

        return sources
        