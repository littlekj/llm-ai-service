from datetime import datetime, timedelta, timezone
from typing import Optional
import jwt  # PyJWT
import secrets
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status, Response, Request
from fastapi.security import OAuth2PasswordBearer
from pydantic import SecretStr

from src.config.settings import settings
from src.models.user import User


jwt_secret_key = settings.JWT_SECRET_KEY.get_secret_value() if isinstance(
    settings.JWT_SECRET_KEY, SecretStr) else settings.JWT_SECRET_KEY


# 密码哈希工具
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def create_hash(value: str) -> str:
    """生成哈希值"""
    return pwd_context.hash(value)

def verify_hash(plain_value: str, hashed_value: str) -> bool:
    """验证哈希值是否匹配"""
    return pwd_context.verify(plain_value, hashed_value)


def verify_refresh_token_in_db(raw_token: str, user: User) -> bool:
    """验证 Refresh Token 是否有效"""
    if not user.refresh_token:  # DB 中没有 Refresh Token
        return False
    
    return verify_hash(plain_value=raw_token, hashed_value=user.refresh_token)


"""TODO:
1. 实现 Token 黑名单（封禁用户时使用）
2. 实现 Refresh Token 的哈希验证机制
"""


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建 JWT 访问令牌"""
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta is None:
        expires_delta = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = now + expires_delta
    to_encode.update({"exp": expire, 'iat': now, "type": "access"})

    return jwt.encode(payload=to_encode, key=jwt_secret_key, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建 JWT 刷新令牌"""
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta is None:
        expires_delta = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    expire = now + expires_delta
    jti = secrets.token_urlsafe(16)  # 生成唯一的 JWT ID
    to_encode.update({"exp": expire, 'iat': now, "type": "refresh", "jti": jti})

    return jwt.encode(payload=to_encode, key=jwt_secret_key, algorithm=settings.JWT_ALGORITHM)

def invalidate_token(token: str, redis_client):
    """使 JWT 令牌失效"""
    pass


def decode_token(token: str):
    """解码并验证 JWT 访问令牌"""
    payload = jwt.decode(
        token, 
        jwt_secret_key, 
        algorithms=[settings.JWT_ALGORITHM]
    )
    return payload


def set_refresh_token_cookie(response: Response, refresh_token: str):
    """在响应中设置 HttpOnly 的 Refresh Token Cookie"""
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=True,  # 生产环境建议启用 HTTPS
        samesite="strict",
        max_age=settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
    )


def clear_refresh_token_cookie(response: Response):
    """清除 Refresh Token Cookie"""
    response.delete_cookie(key="refresh_token")
