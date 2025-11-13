from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator
from typing import Optional
from uuid import UUID
import jwt  # PyJWT

from src.core.database import get_sync_db
from src.core.database import get_async_db
from src.models.user import User
from src.core import security
from src.services.document_service import DocumentService
from src.services.chat_service import ChatService
from src.services.session_service import SessionService
from src.utils.llm_client import LLMClient, get_llm_client
from src.utils.qdrant_storage import QdrantClient, get_vector_store
from src.crud.document import DocumentCRUD, get_document_dao
from src.crud.chat import ChatCRUD, get_chat_dao
from src.crud.user import UserCRUD, get_user_dao



# OAuth2 认证方式：Bearer Token
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="http://localhost:8000/api/v1/auth/login",
    auto_error=False  # 当没有认证时不自动抛出错误
)
     

def get_sync_session():
    with get_sync_db() as db:
        yield db
        
 
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_async_db() as db:
        yield db
 

async def get_current_user(
    token: str = Depends(oauth2_scheme), 
    db: Session = Depends(get_sync_session)
):
    """
    通过 JWT 令牌获取当前用户
    token: 从请求头获取的 Bearer Token
    db: 数据库会话
    """
    try:
        payload = security.decode_token(token)
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user_id: UUID = UUID(payload.get("sub"))
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

async def get_optional_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_sync_session),
) -> Optional[User]:
    """
    尝试获取当前用户，但如果未认证则返回 None
    - 对于已登录用户，返回用户对象
    - 对于游客（未登录），返回 None
    """
    if not token:
        return None
        
    try:
        return await get_current_user(token, db)
    except HTTPException:
        return None  # 认证失败时静默返回 None

async def get_document_service(
    document_crud: DocumentCRUD = Depends(get_document_dao)
):
    return DocumentService(document_crud)

async def get_chat_service(
    llm_client: LLMClient = Depends(get_llm_client),
    vector_store: QdrantClient = Depends(get_vector_store),
    chat_crud: ChatCRUD = Depends(get_chat_dao),
    user_crud: UserCRUD = Depends(get_user_dao),
):
    return ChatService(llm_client, vector_store, chat_crud, user_crud)

async def get_session_service(
    chat_crud: ChatCRUD = Depends(get_chat_dao),
):
    return SessionService(chat_crud)