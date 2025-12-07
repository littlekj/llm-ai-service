import logging
from fastapi import APIRouter, Depends, Header, Response
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from uuid import UUID


from src.models.user import User
from src.core.depends import get_async_session
from src.core.depends import get_current_user
from src.core.depends import get_optional_user
from src.core.depends import get_chat_service, get_chat_dao
from src.services.chat_service import ChatService
from src.schemas.chat import QuestionRequest, QuestionResponse
from src.schemas.session import SessionResponse
from src.crud.chat import ChatCRUD

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["chat"])

@router.post("/ask", response_model=QuestionResponse)
async def ask_question(
    question: QuestionRequest,
    response: Response,
    db: AsyncSession = Depends(get_async_session),
    current_user: Optional[User] = Depends(get_optional_user),
    chat_service: ChatService = Depends(get_chat_service),
    chat_crud: ChatCRUD = Depends(get_chat_dao),
    client_id: UUID = Header(..., alias="X-Client-ID"),  # 前端传入
    guest_session_id: UUID = Header(None, alias="X-Guest-Session-ID"),  # 前端传入
):
    """
    问答接口
    - 支持游客模式（通过 X-Guest-Session-ID）
    - 支持登录用户：登录用户首次请求时，自动继承其设备上的游客会话（通过 client_id）
    """
    # logger.info(f"client_id: {client_id}")
    # logger.info(f"guest_session_id: {guest_session_id}")
    
    logger.info(f"Received to ask question request")
    
    if current_user:
         # 迁移游客会话到登录用户
        migrated_count = await chat_crud.attach_session_to_user_async(
            db=db,
            client_id=client_id,
            user_id=current_user.id
        )
        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} sessions to user {current_user.id}")
    
    # 会话 ID 处理逻辑
    session_id = None
    if current_user and question.session_id:
        # 已登录用户：使用请求中指定的会话（如果有）
        session_id = question.session_id
    elif not current_user and guest_session_id:
        # 游客模式：尝试使用 header 中的会话 ID
        session_id = guest_session_id
            
    # 如果都没有，保持 session_id = None，创建新会话
    result = await chat_service.get_answer(
        db=db,
        user=current_user,
        question=question.question,
        document_ids=question.document_ids,
        session_id=session_id,
        client_id=client_id
    )

    # 如果是游客（未登录），把会话 ID 返回给前端，方便前端保存并在用户注册/登录后绑定
    if not current_user and result.session_id:
        response.headers["X-Guest-Session-ID"] = str(result.session_id)

    return result
