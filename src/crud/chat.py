import logging
from typing import Optional, List, Tuple, Dict, Any
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import NoResultFound

from src.models.chat import ChatSession, ChatMessage, ChatCall


logger = logging.getLogger(__name__)
class ChatCRUD():
    """
    处理聊天会话数据访问的类
    """
    
    async def get_session_by_id_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        user_id: Optional[UUID],
        client_id: UUID,   
    ) -> Optional[ChatSession]:
        """根据 ID 获取会话"""
        try:
            conditions = [ChatSession.id == session_id]
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.extend([
                    ChatSession.user_id.is_(None), 
                    ChatSession.client_id == client_id
                ])
            stmt = select(ChatSession).where(*conditions)
            result = await db.execute(stmt)
            
            return result.scalars().first()
        
        except SQLAlchemyError as e:
            logger.error(f"Database query error: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query error: {str(e)}", exc_info=True)
            raise
        
    async def get_multi_sessions_by_user_async(
        self,
        db: AsyncSession,
        user_id: Optional[UUID],
        client_id: UUID,
        skip: int = 0,
        limit: int = 10,
    ) -> Optional[List[Tuple[ChatSession, int]]]:
        """获取用户的会话列表"""
        try:   
            conditions = [ChatSession.user_id == user_id]
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.extend([
                    ChatSession.user_id.is_(None),
                    ChatSession.client_id == client_id
                ])
            
            stmt = (
                select(
                    ChatSession,
                    func.count(ChatMessage.id).label("message_count")
                )
                .outerjoin(ChatMessage).where(*conditions)
                .group_by(ChatSession.id).offset(skip).limit(limit)
            )
            
            result = await db.execute(stmt)
            return [(session, count) for session, count in result.all()]
            
        except SQLAlchemyError as e:
            logger.error(f"Database query sessions error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected query sessions error: {str(e)}")
            raise
        
    async def get_messages_by_session_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        user_id: Optional[UUID],
        client_id: UUID,
        skip: int = 0,
        limit: int = 20,
    ) -> Tuple[List[ChatMessage], int]:
        try:
            conditions = [ChatSession.id == session_id]
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.extend([
                    ChatSession.user_id.is_(None),
                    ChatSession.client_id == client_id
                ])
                
            stmt = (
                select(ChatMessage).select_from(ChatMessage)
                .join(ChatSession, ChatSession.id == ChatMessage.session_id)
                .where(*conditions)
                .order_by(ChatMessage.created_at.desc())
                .offset(skip).limit(limit)
            )
        
            result = await db.execute(stmt)
            messages = result.scalars().all()
            
            count_stmt = (
                select(func.count()).select_from(ChatMessage)
                .join(ChatSession, ChatSession.id == ChatMessage.session_id)
                .where(*conditions)
            )
            total_result = await db.execute(count_stmt)
            total = total_result.scalar() or 0
            
            return list(messages), total
    
        except SQLAlchemyError as e:
            logger.error(f"Database query messages error: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected query messages error: {str(e)}", exc_info=True)
            raise
        
    async def create_session_async(
        self,
        db: AsyncSession,
        id: Optional[UUID],
        client_id: UUID,
        obj_in: dict,
    ):
        try:
            session = ChatSession(
                id=id,
                client_id=client_id,
                **obj_in,
            )
            db.add(session)
            await db.commit()
            await db.refresh(session)
            
            return session
    
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during create session: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during create session: {str(e)}", exc_info=True)
            raise
        
    async def delete_session_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        user_id: Optional[UUID],
        client_id: UUID,
    ) -> bool:
        """根据 ID 删除会话"""
        try:
            # 构建基础查询条件
            conditions = [ChatSession.id == session_id]
            
            # 添加用户权限验证
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.append(ChatSession.user_id.is_(None))
                conditions.append(ChatSession.client_id == client_id)
            
            # 执行删除操作
            stmt = delete(ChatSession).where(*conditions)
            result = await db.execute(stmt)
            
            # 检查是否成功删除
            if result.rowcount == 0:
                logger.warning(f"Session not found or not permission to delete")
                return False
           
            await db.commit()
            return True
        
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during delete session: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during delete session: {str(e)}", exc_info=True)
            raise

    async def attach_session_to_user_async(
        self,
        db: AsyncSession,
        # session_id: UUID,
        client_id: UUID,
        user_id: UUID,
    ) -> int:
        """将未关联用户的会话关联到用户"""
        try:
            stmt = select(ChatSession).where(
                ChatSession.client_id == client_id,
                ChatSession.user_id.is_(None)
            ).with_for_update()

            result = await db.execute(stmt)
            sessions = result.scalars().all()
            
            if not sessions:
                return 0
            
            stmt = update(ChatSession).where(
                ChatSession.client_id == client_id,
                ChatSession.user_id.is_(None)
            ).values(user_id=user_id)
            await db.execute(stmt)
            await db.commit()
            
            return len(sessions)

        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during attach session to user: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during attach session to user: {str(e)}", exc_info=True)
            raise  
           
    async def update_session_token_stats_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
    ) -> bool:
        """更新会话的 token 统计
        
        Args:
            db: 数据库会话
            session_id: 会话ID
            prompt_tokens: 新增的 prompt tokens
            completion_tokens: 新增的 completion tokens
            total_tokens: 新增的总 tokens
        """
        try:
            stmt = (
                update(ChatSession)
                .where(ChatSession.id == session_id)
                .values(
                    prompt_tokens=ChatSession.prompt_tokens + prompt_tokens,
                    completion_tokens=ChatSession.completion_tokens + completion_tokens,
                    total_tokens=ChatSession.total_tokens + total_tokens,
                    updated_at=func.now()
                )
            )
            await db.execute(stmt)
            await db.commit()
            return True
            
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error updating session token stats: {str(e)}", exc_info=True)
            raise
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error updating session token stats: {str(e)}", exc_info=True)
            raise
        
    async def create_message_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        role: str,
        content: str,
        used_tokens: int,
    ) -> ChatMessage:
        """创建并保存一条消息"""
        try:
            msg = ChatMessage(
                session_id=session_id,
                role=role,
                content=content,
                used_tokens=used_tokens
            )
            db.add(msg)
            await db.commit()
            await db.refresh(msg)
            
            return msg
        
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during create chat message: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during create chat message: {str(e)}", exc_info=True)
            raise
        
    async def get_message_async(
        self,
        db: AsyncSession,
        message_id: UUID,
        user_id: Optional[UUID],
        client_id: UUID,
    ) -> Optional[ChatMessage]:
        """获取消息"""
        try:
            conditions = [ChatMessage.id == message_id]
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.append(ChatSession.user_id.is_(None))
                conditions.append(ChatSession.client_id == client_id)
            stmt = (
                select(ChatMessage)
                .join(ChatSession, ChatSession.id == ChatMessage.session_id)
                .where(*conditions)
            )
            
            result = await db.execute(stmt)
            message = result.scalar_one_or_none()
            
            return message
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during get chat message: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during get chat message: {str(e)}", exc_info=True)
            raise
        
    async def get_recent_messages_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        client_id: UUID,
        user_id: Optional[UUID],
        limit: int = 5,
    ):
        """获取最近的对话历史"""
        try:
            # 构建基础查询条件
            conditions = [ChatSession.id == session_id]
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.append(ChatSession.user_id.is_(None))
                conditions.append(ChatSession.client_id == client_id)
                
            # 使用 join 直接查询消息，避免加载整个会话
            stmt = select(ChatMessage).join(ChatSession).where(
                *conditions,
            ).order_by(ChatMessage.created_at.desc()).limit(limit)
            
            result = await db.execute(stmt)
            messages = result.scalars().all()
            
            # 按逆序返回消息列表
            return list(reversed(messages))
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during get recent messages: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during get recent messages: {str(e)}", exc_info=True)
            raise
        
    async def delete_message_async(
        self,
        db: AsyncSession,
        message_id: UUID,
        user_id: Optional[UUID],
        client_id: UUID,
    ) -> bool:
        """删除消息，同时验证权限
        
        Args:
            db: 数据库会话
            message_id: 要删除的消息ID
            user_id: 用户ID（对于已登录用户）
            client_id: 客户端ID（对于游客模式）
        """
        try:
            # 构建基础查询条件（确保消息和其所属的会话都符合权限要求）
            conditions = [ChatSession.id == ChatMessage.session_id]  # 关联会话
            if user_id is not None:
                conditions.append(ChatSession.user_id == user_id)
            else:
                conditions.append(ChatSession.user_id.is_(None))
                conditions.append(ChatSession.client_id == client_id)
            
            # 使用子查询获取有权限访问的会话 ID
            allowed_session_ids = select(ChatSession.id).where(*conditions)
            
            # 主查询删除符合条件的消息
            stmt = (
                delete(ChatMessage)
                .where(ChatMessage.id == message_id)
                .where(ChatMessage.session_id.in_(allowed_session_ids))
            )
            
            result = await db.execute(stmt)
            if result.rowcount == 0:
                # 如果没有删除任何行，说明消息不存在或没有权限
                return False
                
            await db.commit()
            return True
        
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during delete chat message: {str(e)}", exc_info=True)
            raise
        
    async def create_call_record_async(
        self,
        db: AsyncSession,
        session_id: UUID,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        latency_ms: int = 0,
    ):
        """创建并保存一次 LLM 调用记录"""
        try:
            call = ChatCall(
                session_id=session_id,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
            )
            
            db.add(call)
            await db.commit()
            await db.refresh(call)
            
            return call

        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error creating chat call: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error creating chat call: {str(e)}", exc_info=True)
            raise
        
        
async def get_chat_dao():
    return ChatCRUD()         
        