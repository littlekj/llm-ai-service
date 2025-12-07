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
from src.crud.user import UserCRUD
from src.crud.document import DocumentCRUD
from src.crud.chat import ChatCRUD
from src.services.user_service import UserService
from src.services.document_service import DocumentService
from src.services.chat_service import ChatService
from src.services.session_service import SessionService
from src.utils.minio_storage import MinioClient
from src.utils.llm_client import LLMClient
from src.utils.qdrant_storage import QdrantClient


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
 
async def get_user_dao():
    return UserCRUD()

async def get_user_service(
    db: AsyncSession = Depends(get_async_session),
    user_crud: UserCRUD = Depends(get_user_dao),
):
    return UserService(db, user_crud)

async def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_session),
    user_crud: UserCRUD = Depends(get_user_dao),
):
    """
    通过 JWT 令牌获取当前用户
    token: 从请求头获取的 Bearer Token, 允许为空以便手动检查
    db: 数据库会话
    """
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload = security.decode_token(token)
        
        if payload.get("type") != "access":
            raise HTTPException(
                status_code=401, 
                detail="Invalid token type",
                headers={"WWW-Authenticate": "Bearer"}
            )
            
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=401, 
                detail="Invalid token payload",
                headers={"WWW-Authenticate": "Bearer"}
            )
           
        user_id = UUID(user_id) 
            
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=401, 
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    user = await user_crud.get_active_user_by_id(db, user_id)
    
    if user is None:
        raise HTTPException(
            status_code=401, 
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return user

async def get_optional_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_session),
    user_crud: UserCRUD = Depends(get_user_dao),
) -> Optional[User]:
    """
    尝试获取当前用户，但如果未认证则返回 None
    - 对于已登录用户，返回用户对象
    - 对于游客（未登录），返回 None
    """
    if not token:
        return None
        
    return await get_current_user(token, db, user_crud)

async def get_document_dao():
    return DocumentCRUD()

async def get_document_service():
    return DocumentService()

def get_minio_client() -> MinioClient:
    """
    获取配置好的 MinioClient 实例
    """
    return MinioClient()

async def get_llm_client() -> LLMClient:
    llm_client = LLMClient()
    return llm_client

def get_vector_store() -> QdrantClient:
    return QdrantClient() 

async def get_chat_dao():
    return ChatCRUD()  

async def get_chat_service(
    chat_crud: ChatCRUD = Depends(get_chat_dao),
    user_crud: UserCRUD = Depends(get_user_dao),
    llm_client: LLMClient = Depends(get_llm_client),
    vector_store: QdrantClient = Depends(get_vector_store), 
):
    return ChatService(llm_client, vector_store, chat_crud, user_crud)

async def get_session_service(
    chat_crud: ChatCRUD = Depends(get_chat_dao),
):
    return SessionService(chat_crud)