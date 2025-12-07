from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.exc import SQLAlchemyError
from pydantic import EmailStr

from src.schemas.user import UserResponse, UserCreate, TokenResponse
from src.schemas.user import PasswordResetRequest, PasswordResetConfirm, PasswordChangeRequest
from src.core.depends import get_sync_session, get_async_session, get_user_service
from src.services.user_service import UserService
from src.core import security
from src.models.user import User
from src.core.depends import get_current_user
import logging


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse, status_code=201)
async def register(
    user_in: UserCreate, 
    user_service: UserService = Depends(get_user_service)
):
    """用户注册"""
    new_user = await user_service.register_user(user_in)  # 调用用户服务创建用户
        
    return new_user
    
@router.post("/confirm-email")
async def confirm_email(
    token: str, 
    user_service: UserService = Depends(get_user_service)
):
    """激活用户账户"""
    await user_service.confirm_email(token)
    
    return {"message": "Email confirmed. You can log in now."}
    
@router.post("/resend-confirmation")
async def resend_confirmation_email(
    email: EmailStr,
    user_service: UserService = Depends(get_user_service)
):
    """重新发送用户激活邮件"""
    
    return await user_service.resend_confirmation_email(email)

@router.post("/login", response_model=TokenResponse)
async def login(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    user_service: UserService = Depends(get_user_service)
):
    """用户登录，返回 JWT 令牌并设置 HttpOnly Cookie"""
    # TODO：添加登录尝试限流
    # check_rate_limit(form_data.username, redis)

    # 根据用户名和密码获取用户    
    return await user_service.authenticate(response, form_data.username, form_data.password)

@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    request: Request, 
    response: Response, 
    user_service: UserService = Depends(get_user_service)
):
    """使用 HttpOnly Cookie 中的 Refresh Token 刷新 Access Token """
    
    logger.info("Refreshing token using HttpOnly Cookie")
    return await user_service.refresh_token(request, response)
    
@router.post("/logout")
async def logout(
    response: Response,
    current_user: User = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
):
    """用户登出，清除 Refresh Token 和 Cookie"""
    logger.info(f"Logging out user {current_user.id}")
    
    # 清除数据库中的 Refresh Token
    await user_service.revoke_refresh_token(current_user)
    
    # 清除 HttpOnly Cookie
    security.clear_refresh_token_cookie(response)
    return {"message": "Logged out successfully"}


@router.post("/forgot")
async def forgot_password(
    request: PasswordResetRequest,
    # background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service)
):
    """
    忘记密码，发送密码重置链接
    注意：无论用户是否存在，都返回相同响应（安全）
    """    
    # 发送密码重置邮件
    await user_service.initiate_password_reset(request.email)
    
    # 无论用户是否存在，都返回相同响应（安全）
    return {"message": "If your email is registered, a reset link has been sent."}
 

@router.post("/reset")
async def reset_password(
    request: PasswordResetConfirm, 
    user_service: UserService = Depends(get_user_service)
):
    """使用 token 重置密码"""
    result = await user_service.reset_password_with_token(request)
    return result
    

@router.post("/change")
async def change_password(
    request: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
):
    """允许已登录用户修改密码"""
    result = await user_service.change_password_for_user(request, current_user)
    return result