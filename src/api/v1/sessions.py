from fastapi import APIRouter, Depends, Header, Query
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from uuid import UUID
import logging

from src.models.user import User
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.core.depends import get_optional_user
from src.core.depends import get_session_service
from src.services.session_service import SessionService
from src.schemas.chat import QuestionRequest, QuestionResponse
from src.schemas.session import SessionResponse
from src.schemas.session import ChatMessageSchema, ChatMessagePaginatedResponse
from src.crud.chat import ChatCRUD
from src.core.depends import get_chat_dao


logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["sessions"])

@router.post("/sessions/attach")
async def attach_session(
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
    chat_crud: ChatCRUD = Depends(get_chat_dao),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
):
    """
    将游客会话绑定到当前用户
    - 如果用户已登录，则将游客会话绑定到该用户
    - 如果用户未登录，则返回 401 错误
    """
    logger.info(f"Attempting to attach guest session to current user")
    
    try:
        # 迁移游客会话到登录用户
        migrated_count = await chat_crud.attach_session_to_user_async(
            db=db,
            client_id=client_id,
            user_id=current_user.id
        )
        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} sessions to user {current_user.id}")
    except Exception as e:
        logger.error(f"Unexpected error during attach session: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sessions", response_model=List[SessionResponse])
async def list_sessions(
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
    session_service: SessionService = Depends(get_session_service),
):
    """获取用户的会话列表"""
    if not current_user:
        logger.warning("User is not authenticated and list sessions for guest")
        
    else:
        logger.info(f"Listing sessions for user {current_user.id}")

    try:
        sessions = await session_service.list_sessions(
            db=db, 
            user=current_user,
            client_id=client_id
        )
        
        if not sessions:
            logger.warning(f"No sessions found for user")
            raise HTTPException(status_code=404, detail="No sessions found")

        return sessions
    
    except Exception as e:
        logger.error(f"Unexpected error during list sessions: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/sessions/{session_id}/messages", response_model=ChatMessagePaginatedResponse[ChatMessageSchema])
async def get_session_history(
    session_id: UUID,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    session_service: SessionService = Depends(get_session_service),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
):
    """获取指定会话的历史记录"""
    logger.info(f"Getting session history for session {session_id}")
    
    if not current_user:
        logger.warning("User is not authenticated and get session history for guest")
    
    try:
        session_history = await session_service.get_session_history(
            db=db,
            session_id=session_id,
            user=current_user,
            client_id=client_id,
            page=page,
            size=size
        )
        
        if not session_history:
            logger.warning(f"No session history found for session {session_id}")
            raise HTTPException(status_code=404, detail="Session not found")
        
        return session_history

    except Exception as e:
        logger.error(f"Unexpected error during get session history: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.delete("/sessions/{session_id}", status_code=204)
async def delete_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    session_service: SessionService = Depends(get_session_service),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
):
    """删除指定会话"""
    if not current_user:
        logger.warning("User is not authenticated and delete session for guest")
    else:
        logger.info(f"Attempting to delete session for current user")
    
    try:
        success = await session_service.delete_session(
            db=db, 
            session_id=session_id,
            user=current_user,
            client_id=client_id
        )
        
        if not success:
            logger.warning(f"Session not found or no permission to delete")
            raise HTTPException(status_code=404, detail="Session not found")

        logger.info(f"Successfully deleted session {session_id}")
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error during delete session: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/messages/{message_id}", response_model=ChatMessageSchema)
async def get_message(
    message_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    session_service: SessionService = Depends(get_session_service),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
):
    """获取指定消息"""
    if not current_user:
        logger.warning("User is not authenticated during get message")
    else:
        logger.info(f"Attempting to get message for current user")

    try:
        message = await session_service.get_message(
            db=db,
            message_id=message_id,
            user=current_user,
            client_id=client_id
        )

        if not message:
            logger.warning(f"No message found with id {message_id}")
            raise HTTPException(status_code=404, detail="Message not found")

        return message

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during get message: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.delete("/messages/{message_id}", status_code=204)
async def delete_message(
    message_id: UUID,
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    session_service: SessionService = Depends(get_session_service),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
):
    """删除指定消息"""
    if not current_user:
        logger.warning("User is not authenticated during delete message")
    else:
        logger.info(f"Attempting to delete message for current user")

    try:
        await session_service.delete_message(
            db=db, 
            message_id=message_id, 
            user=current_user, 
            client_id=client_id
        )

        logger.info(f"Successfully deleted message {message_id}")
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during delete message: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")