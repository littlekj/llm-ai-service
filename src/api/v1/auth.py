from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.exc import SQLAlchemyError
from pydantic import EmailStr

from src.schemas.user import UserResponse, UserCreate, TokenResponse
from src.schemas.user import PasswordResetRequest, PasswordResetConfirm, PasswordChangeRequest
from src.core.depends import get_sync_session, get_async_session
from src.services.user_service import UserService
from src.core import security
from src.core.exceptions import UserAlreadyExistsError, AuthenticationError
from src.models.user import User
from src.core.depends import get_current_user
import logging


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse, status_code=201)
async def register(user_in: UserCreate, db: AsyncSession = Depends(get_async_session)):
    """用户注册"""
    service = UserService(db)  # 实例化用户服务
    try:
        new_user = await service.register_user(user_in)  # 调用用户服务创建用户
        return new_user
    except UserAlreadyExistsError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # 未知错误统一转 500
        logger.exception(f"DB error during registration: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@router.post("/confirm-email")
async def confirm_email(token: str, db: AsyncSession = Depends(get_async_session)):
    """激活用户账户"""
    user_service = UserService(db)
    try:
        await user_service.confirm_email(token)
        return {"message": "Email confirmed. You can log in now."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error during email confirmation: {e}")
        raise HTTPException(status_code=500, detail="Failed to confirm email")
    
@router.post("/resend-confirmation")
async def resend_confirmation_email(
    email: EmailStr,
    db: AsyncSession = Depends(get_async_session)
):
    """重新发送用户激活邮件"""
    user_service = UserService(db)
    try:
        await user_service.resend_confirmation_email(email)
        return {"message": "Confirmation email has been resent."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error during email resend: {e}")
        raise HTTPException(status_code=500, detail="Failed to resend confirmation email")

@router.post("/login", response_model=TokenResponse)
def login(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_sync_session)
):
    """用户登录，返回 JWT 令牌并设置 HttpOnly Cookie"""
    # TODO：添加登录尝试限流
    # check_rate_limit(form_data.username, redis)

    # 根据用户名和密码获取用户
    service = UserService(db)
    try:
        user = service.authenticate(form_data.username, form_data.password)
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # 创建访问令牌和刷新令牌
    access_token = security.create_access_token(data={"sub": str(user.id)})
    refresh_token = security.create_refresh_token(data={"sub": str(user.id)})

    # 存储 refresh_token（用于封禁或单点登录）
    user.refresh_token = security.create_hash(refresh_token)
    db.commit()

    # 写入 HttpOnly Cookie
    security.set_refresh_token_cookie(response, refresh_token)

    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/refresh", response_model=TokenResponse)
def refresh_token(request: Request, response: Response, db: Session = Depends(get_sync_session)):
    """使用 HttpOnly Cookie 中的 Refresh Token 刷新 Access Token """
    # 获取 HttpOnly Cookie 中的 Refresh Token
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Refresh token missing")

    service = UserService(db)
    try:
        new_access_token, new_refresh_token, user_id = service.refresh_token(refresh_token)
        
        # 写入 HttpOnly Cookie
        security.set_refresh_token_cookie(response, new_refresh_token)
        
        logger.info(f"Token refreshed for user {user_id}")
        return {"access_token": new_access_token, "token_type": "bearer"}

    except HTTPException:
        raise  # 已经是 HTTPException，直接抛出
    except SQLAlchemyError as e:
        logger.error(f"DB error during token refresh: {e}")
        raise HTTPException(status_code=500, detail="Token refresh failed")
    
    
@router.post("/logout")
def logout(
    response: Response,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_session)
):
    """用户登出，清除 Refresh Token 和 Cookie"""
    logger.info(f"User {current_user.username} initiating logout")

    service = UserService(db)
    try:
        # 清除数据库中的 Refresh Token
        service.revoke_refresh_token(current_user)
        # 清除 HttpOnly Cookie
        security.clear_refresh_token_cookie(response)
        return {"message": "Logged out successfully"}

    except SQLAlchemyError as e:
        db.rollback
        raise HTTPException(status_code=500, detail="Logout failed")


@router.post("/forgot")
def forgot_password(
    request: PasswordResetRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_sync_session)
):
    """
    忘记密码，发送密码重置链接
    注意：无论用户是否存在，都返回相同响应（安全）
    """
    service = UserService(db)
    
    try:
        # 发送密码重置邮件
        service.initiate_password_reset(request.email, background_tasks)
         # 无论用户是否存在，都返回相同响应（安全）
        return {"message": "If your email is registered, a reset link has been sent."}
    except Exception as e:
        logger.exception(f"Unexpected error during password reset request")
        return {"message": "If your email is registered, a reset link has been sent."}
 

@router.post("/reset")
def reset_password(request: PasswordResetConfirm, db: Session = Depends(get_sync_session)):
    """使用 token 重置密码"""
    service = UserService(db)

    try:
        result = service.reset_password_with_token(request.token, request.new_password)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError:
        raise HTTPException(status_code=500, detail="Password reset failed due to server error")
    

@router.post("/change")
def change_password(
    request: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_sync_session)
):
    """允许已登录用户修改密码"""
    service = UserService(db)
    try:
        result = service.change_password_for_user(current_user, request.old_password, request.new_password)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError:
        raise HTTPException(status_code=500, detail="Password change failed due to server error")