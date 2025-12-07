import logging
from sqlalchemy.ext.asyncio import  AsyncSession
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

from src.crud.chat import ChatCRUD
from src.models.user import User
from src.schemas.session import SessionResponse
from src.schemas.session import ChatMessageSchema, ChatMessagePaginatedResponse
from src.schemas.pagination import create_pagination_response
from src.core.exceptions import DatabaseError, NotFoundError

logger = logging.getLogger(__name__)


class SessionService:
    def __init__(
        self,
        chat_crud: ChatCRUD,
    ):
        self.chat_crud = chat_crud
        
    async def list_sessions(
        self,
        db: AsyncSession,      # 异步数据库会话，用于数据库操作
        user: Optional[User],  # 用户对象，包含用户信息
        client_id: UUID,      # 客户端唯一标识
        skip: int = 0,        # 跳过的记录数，用于分页
        limit: int = 10,      # 限制返回的记录数，用于分页
    ) -> List[SessionResponse]:
        """获取用户会话列表"""
        try:
            sessions = await self.chat_crud.get_multi_sessions_by_user_async(
                db,
                user_id=user.id if user else None,
                client_id=client_id,
                skip=skip,
                limit=limit,
            )
            
            if not sessions:
                return []
            
            # 构建会话响应对象列表
            return [
                SessionResponse(
                    id=session.id,
                    user_id=session.user_id,
                    client_id=session.client_id,
                    title=session.title,
                    created_at=session.created_at,
                    updated_at=session.updated_at,
                    message_count=message_count  
                ) for session, message_count in sessions
            ]
            
        except SQLAlchemyError as e:
            logger.error(f"Database error during list sessions: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to list sessions due to database error") from e
        except Exception as e:
            logger.error(f"Unexpected error during list sessions: {str(e)}", exc_info=True)
            raise DatabaseError(message="Unexpected error occurred while listing sessions") from e
            
   
    async def get_session_history(
        self,
        db: AsyncSession,
        session_id: UUID,
        user: Optional[User],
        client_id: UUID,
        page: int = 1,
        size: int = 20,
    ) -> ChatMessagePaginatedResponse[ChatMessageSchema]:
        """获取会话历史记录"""
        skip = (page - 1) * size
        try:
            messages, total = await self.chat_crud.get_messages_by_session_async(
                db=db, 
                session_id=session_id, 
                user_id=user.id if user else None,
                client_id=client_id,
                skip=skip,
                limit=size,
            )
            
            # 将消息对象转换为响应模式
            messages_schemas = [
                ChatMessageSchema(
                    id=msg.id,
                    session_id=msg.session_id,
                    role=msg.role,
                    content=msg.content,
                    created_at=msg.created_at,
                    used_tokens=msg.used_tokens,
                ) for msg in messages
            ]
            
            # 返回分页响应
            return create_pagination_response(
                items=messages_schemas,
                total=total,
                page=page,
                size=size,
                response_model=ChatMessagePaginatedResponse[ChatMessageSchema]
            )
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during get session history: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to get session history due to database error") from e
        except Exception as e:
            logger.error(f"Unexpected error during get session history: {str(e)}", exc_info=True)
            raise DatabaseError(message="Unexpected error occurred while getting session history") from e
    
    async def delete_session(
        self,
        db: AsyncSession,
        user: Optional[User],
        session_id: UUID,
        client_id: UUID,
    ) -> bool:
        """删除会话"""               
        try:
            # # 验证会话存在性和权限
            # session = await self.chat_crud.get_session_by_id_async(
            #     db=db, 
            #     session_id=session_id, 
            #     user_id=user.id,
            #     client_id=client_id
            # )
            
            # if not session:
            #     raise ValueError(f"Session not found or no permission to delete")
        
            # 执行删除操作
            result = await self.chat_crud.delete_session_async(
                db=db, 
                session_id=session_id, 
                user_id=user.id if user else None,
                client_id=client_id
            )
            
            if not result:
                raise NotFoundError(resource="Session", resource_id=str(session_id))
            
            await db.commit()
            return True
        
        except NotFoundError:
            raise
        except IntegrityError as e:
            await db.rollback()
            logger.error(f"Integrity error during delete session: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to delete session due to integrity error") from e
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during delete session: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to delete session due to database error") from e
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during delete session: {str(e)}", exc_info=True)
            raise DatabaseError(message="Unexpected error occurred while deleting session") from e
        
    async def get_message(
        self,
        db: AsyncSession,
        message_id: UUID,
        user: Optional[User],
        client_id: UUID,
    ) -> Optional[ChatMessageSchema]:
        """获取消息"""
        try:
            message = await self.chat_crud.get_message_async(
                db=db,
                message_id=message_id,
                user_id=user.id if user else None,
                client_id=client_id,
            )
            
            if not message:
                raise NotFoundError(resource="Message", resource_id=str(message_id))

            return ChatMessageSchema(
                id=message.id,
                session_id=message.session_id,
                role=message.role,
                content=message.content,
                created_at=message.created_at,
                used_tokens=message.used_tokens 
            )
            
        except NotFoundError:
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error during get message: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to get message due to database error") from e
        except Exception as e:
            logger.error(f"Unexpected error during get message: {str(e)}", exc_info=True)
            raise
        
    async def delete_message(
        self,
        db: AsyncSession,
        message_id: UUID,
        user: Optional[User],
        client_id: UUID,
    ) -> bool:
        """删除消息
        
        Args:
            db: 数据库会话
            message_id: 要删除的消息ID
            user: 用户对象（游客模式下为None）
            client_id: 客户端ID
        """
        try:
            # 删除消息，同时验证权限
            result = await self.chat_crud.delete_message_async(
                db=db,
                message_id=message_id,
                user_id=user.id if user else None,
                client_id=client_id,
            )
            
            if not result:
                raise NotFoundError(resource="Message", resource_id=str(message_id))
            
            await db.commit()
                
            return True
        
        except NotFoundError:
            raise 
        except IntegrityError as e:
            await db.rollback()
            logger.error(f"Integrity error during delete message: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to delete message due to integrity error") from e
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Database error during delete message: {str(e)}", exc_info=True)
            raise DatabaseError(message="Failed to delete message due to database error") from e
        except Exception as e:
            await db.rollback()
            logger.error(f"Unexpected error during delete message: {str(e)}", exc_info=True)
            raise DatabaseError(message="Unexpected error occurred while deleting message") from e
            