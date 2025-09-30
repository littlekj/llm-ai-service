from pydantic import BaseModel, EmailStr, Field
from typing import Annotated
from datetime import datetime
from enum import Enum


# 强制密码 8-128 位，避免弱口令
PasswordStr = Annotated[str, Field(min_length=8, max_length=128)]  


class UserCreate(BaseModel):
    """用户注册请求模型"""
    username: str
    email: EmailStr
    password: PasswordStr
   
    
class UserLogin(BaseModel):
    """用户登录请求模型"""
    email: EmailStr
    password: str
    
class UserRole(str, Enum):
    """用户角色枚举"""
    ADMIN = "admin"
    USER = "user"
    GUEST = "guest"
  
class UserResponse(BaseModel):
    """用户响应模型（返回给客户端）"""
    id: int
    username: str
    email: EmailStr    # 自动校验 email 格式
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    model_config = {
        "from_attributes": True  # 支持从 ORM 对象读取属性（替代 v1 的 orm_mode）
    }
  
    
class TokenResponse(BaseModel):
    """JWT 令牌响应模型"""
    access_token: str
    token_type: str = "bearer"
    
    
class PasswordResetRequest(BaseModel):
    """密码重置请求模型"""
    email: EmailStr
    
    
class PasswordResetConfirm(BaseModel):
    """密码重置确认模型"""
    token: str
    new_password: PasswordStr
    

class PasswordChangeRequest(BaseModel):
    """密码修改模型"""
    old_password: str
    new_password: PasswordStr