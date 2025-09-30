from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator
import jwt  # PyJWT

from src.core.database import get_sync_db
from src.core.database import get_async_db
from src.models.user import User
from src.core import security
from src.services.document_service import DocumentService


# OAuth2 认证方式：Bearer Token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://localhost:8000/api/v1/auth/login")
     

def get_sync_session():
    with get_sync_db() as db:
        yield db
        
 
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_async_db() as db:
        yield db
 

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_sync_session)):
    """
    通过 JWT 令牌获取当前用户
    token: 从请求头获取的 Bearer Token
    db: 数据库会话
    """
    try:
        payload = security.decode_token(token)
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

async def get_document_service():
    
    return DocumentService()