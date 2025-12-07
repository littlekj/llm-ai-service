import asyncio
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from fastapi import HTTPException, Request, Response, Depends
from fastapi import BackgroundTasks
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from uuid import UUID
from jwt import ExpiredSignatureError, InvalidTokenError
from resend.exceptions import ResendError
from requests.exceptions import ConnectionError, Timeout, RequestException

from src.schemas.user import UserCreate
from src.schemas.user import PasswordResetConfirm, PasswordChangeRequest
from src.models.user import User
from src.crud.user import UserCRUD
from src.core import security
from src.config.settings import settings
from src.utils import mailer
from src.workers.user.email_notification import (
    send_confirmation_email_task, 
    send_reset_email_task
)
from src.core.exceptions import (
    ValidationError, 
    BusinessLogicError,
    AuthenticationError, 
    NotFoundError,
    ResourceConflictError,
    DatabaseError,
    ExternalServiceError,
)

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self, db, user_crud: UserCRUD):
        self.db = db
        self.user_crud = user_crud

    async def register_user(
        self, user_in: UserCreate,
    ):
        """
        注册新用户，处理用户名/邮箱冲突和未激活账户
        """
        new_user = None
        
        try:
            # 检查是否存在冲突的用户
            active_user, inactive_user, deleted_user = await self.user_crud.check_existing_user(
                db=self.db,
                email=user_in.email,
                username=user_in.username
            )
            
            if active_user:
                # 用户名或邮箱已注册
                if active_user.email == user_in.email:
                    raise ResourceConflictError(message="Email already registered")
                else:
                    raise ResourceConflictError(message="Username already taken")
            
            elif deleted_user:
                # 删除账户后30天内不允许重新注册
                threshold = datetime.now(timezone.utc) - timedelta(days=30)
                
                if deleted_user.deleted_at and deleted_user.deleted_at > threshold:
                    raise ResourceConflictError(message="Account deleted within 30 days")
                
                new_user = deleted_user
                    
            elif inactive_user:
                # 未激活账户7天内允许重新激活
                logger.info(f"Found inactive user: {inactive_user.email}")
                threshold = datetime.now(timezone.utc) - timedelta(days=7)
                
                if inactive_user.created_at < threshold:
                    raise ResourceConflictError(message="Account not activated for more than 7 days")
                
                new_user = inactive_user 

            else:
                # 创建新用户
                logger.info(f"Creating new user: {user_in.email}")
                # 密码哈希处理
                hashed_pw = security.create_hash(user_in.password)
                # 新用户注册后暂不激活，需要邮箱确认
                new_user = await self.user_crud.create_user_async(
                    self.db,
                    user_in,
                    hashed_pw,
                )
                await self.db.commit()
 
        except ResourceConflictError:
            raise
        except IntegrityError as e:
            self.db.rollback()
            logger.error(f"IntegrityError during user registration: {e}", exc_info=True)
            raise DatabaseError(message="User registration failed due to integrity constraint")
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"SQLAlchemyError during user registration: {e}", exc_info=True)
            raise DatabaseError(message="User registration failed due to database error")
        except Exception as e:
            logger.exception(f"Failed to schedule/send confirmation email: {e}")
            raise
        
        # 发送用户注册确认邮件
        await self._send_activation_email(new_user)
        
        return new_user
    
    async def confirm_email(
        self, 
        token: str,
    ) -> dict[str, str]:
        """确认用户邮箱并激活账户"""
        try:
            payload = security.decode_token(token)
            
            if payload.get("scope") != "email_confirm":
                raise AuthenticationError(message="Invalid token scope")
            
            user_id_str = payload.get("sub")
            
            if not user_id_str:
                raise AuthenticationError(message="Token missing subject")
            user_id = UUID(user_id_str)  
            
        except (ExpiredSignatureError, InvalidTokenError) as e:
            # token 过期或无效
            logger.warning(f"Email confirmation failed due to invalaid/expired token: {e}")
            raise AuthenticationError(message="Invalid or expired confirmation token")
        except ValueError:
            # token 中用户ID无法解析为 UUID
            logger.warning("Email confirmation failed due to invalid user ID in token")
            raise AuthenticationError(message="Invalid user ID in token")
        except Exception as e:
            logger.error(f"Unexpected error decoding token: {e}", exc_info=True)
            raise AuthenticationError(message="Failed to decode confirmation token")
        
        try:
            result = await self.user_crud.active_user_async(self.db, user_id)
            
            if result:
                # 成功更新数据，提交事务
                await self.db.commit()
                return {"message": "Email confirmed"}
            
            user = await self.user_crud.get_active_user_by_id(self.db, user_id)
            if user:
                # 用户已存在且激活
                logger.warning(f"User already active for email confirmation: {user_id}")
                return {"message": "Email already confirmed"}
            
            # 用户不存在
            logger.warning(f"User not found for email confirmation: {user_id}")
            raise NotFoundError(resource="User", resource_id=user_id)

        except NotFoundError:
            raise
        except IntegrityError as e:
            await self.db.rollback()
            logger.error(f"IntegrityError activating user {user_id}: {e}")
            raise DatabaseError(message="Failed to activate user due to integrity constraint")
        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"SQLAlchemyError activating user {user_id}: {e}")
            raise DatabaseError(message="Failed to activate user due to database error")
        except Exception as e:
            await self.db.rollback()
            logger.exception(f"Unexpected error activating user {user_id}: {e}")
            raise DatabaseError(message="Failed to activate user")

        
    async def resend_confirmation_email(
        self,
        email: str,
    ):
        """重新发送邮箱确认邮件"""   
        try:
            user = await self.user_crud.get_user_by_email(self.db, email)
            
            if not user:
                logger.info(f"Resend email requested for non-existent user: {email}")
                return {"message": "If the email is registered, you will receive a confirmation email shortly."}

            if user.is_active and user.email_confirmed_at:
                return {"message": "Email already confirmed"}
        
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError fetching user by email {email}: {e}")
            raise DatabaseError(message="Failed to fetch user due to database error")
        except Exception as e:
            logger.exception(f"Unexpected error fetching user by email {email}: {e}")
            raise DatabaseError(message="Failed to fetch user")
             
        await self._send_activation_email(user)
        
        return {"message": "Confirmation email sent"}
            
    async def _send_activation_email(self, new_user: User):
        """生成邮件确认 token（scope=email_confirm）并发送确认邮件"""
        
        confirm_token = security.create_access_token(
            data={"sub": str(new_user.id), "scope": "email_confirm"},
            expires_delta=timedelta(minutes=15.0)
        )
        confirm_url = f"{settings.FRONTEND_CONFIRM_URL}/confirm-email?token={confirm_token}"
            
        try:
            # 使用 celery 发送邮件（如果可用）
            send_confirmation_email_task.delay(new_user.email, confirm_url)
            logger.info(f"Scheduled confirmation email for {new_user.email}")
            
        except Exception as e:
            # 降级为同步发送邮件
            logger.warning(f"Celery not available, sending confirmation email synchronously: {e}")
            
            try:
                # 使用 run_in_executor 避免阻塞事件循环
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    mailer.send_confirmation_email,
                    new_user.email,
                    confirm_url
                )
                logger.info(f"Confirmation email sent synchronously for {new_user.email}")
                
            except ResendError as re:
                logger.error(f"ResendError sending confirmation email to {new_user.email}: {re}", exc_info=True)
            except (ConnectionError, Timeout, RequestException) as e:
                logger.error(f"Network error sending confirmation email to {new_user.email}: {e}", exc_info=True)            
            except Exception as e:
                logger.exception(f"Unexpected error sending confirmation email to {new_user.email}: {e}")


    async def authenticate(
        self, 
        response: Response,
        username_or_email: str, 
        password: str,
    ) -> User:
        """支持用户名或邮箱登录登录"""
        try:
            # 获取用户
            user = await self.user_crud.get_user(
                db=self.db,
                username_or_email=username_or_email,
            )
            
            verified_result = security.verify_hash(plain_value=password, hashed_value=user.hashed_password)
            
            # 统一错误信息，防止枚举攻击
            if user and user.is_active is False:
                raise AuthenticationError(message="Invalid credentials")
            if not user or not verified_result:
                raise AuthenticationError(message="Invalid credentials")
            
            # 创建访问令牌和刷新令牌
            access_token = security.create_access_token(data={"sub": str(user.id)})
            refresh_token = security.create_refresh_token(data={"sub": str(user.id)})

            # 存储 refresh_token（用于封禁或单点登录）
            user.refresh_token = security.create_hash(value=refresh_token)
            await self.db.commit()

            # 写入 HttpOnly Cookie
            security.set_refresh_token_cookie(response=response, refresh_token=refresh_token)

            return {"access_token": access_token, "token_type": "bearer"}
        
        except AuthenticationError:
            raise
        except IntegrityError as e:
            await self.db.rollback()
            logger.error(f"IntegrityError during authentication for {username_or_email}: {e}")
            raise DatabaseError(message="Authentication failed due to integrity constraint")
        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"SQLAlchemyError during authentication for {username_or_email}: {e}")
            raise DatabaseError(message="Authentication failed due to database error")
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Unexpected error during authentication for {username_or_email}: {e}")
            raise DatabaseError(message="Authentication failed due to unexpected error")


    async def refresh_token(
        self, 
        request: Request,
        response: Response,
    ) -> tuple[str, str, UUID]:
        """刷新 access_token 和 refresh_token"""
        try:
            # 获取 HttpOnly Cookie 中的 Refresh Token
            raw_refresh_token = request.cookies.get("refresh_token")
            if not raw_refresh_token:
                raise NotFoundError(resource="Refresh Token", resource_id="N/A")
    
            # 解码并验证 refresh_token
            try:
                payload = security.decode_token(token=raw_refresh_token)
            except Exception:
                raise AuthenticationError(message="Invalid or expired refresh token")
            
            if payload.get("type") != "refresh":
                raise AuthenticationError(message="Invalid token type")
            
            user_id_str = payload.get("sub")
            if not user_id_str:
                raise AuthenticationError(message="Token missing subject")
            user_id = UUID(user_id_str)
            
            jti = payload.get("jti")
            if not jti:
                raise AuthenticationError(message="Token missing jti")

            # 查询用户
            user = await self.user_crud.get_active_user_by_id(self.db, user_id)
            if not user:
                raise NotFoundError(resource="User", resource_id=user_id)
            
            verified_result = security.verify_refresh_token_in_db(raw_refresh_token, user)
            
            if not user or not verified_result:
                raise AuthenticationError(message="Invalid credentials")
            
            # 生成新的 access_token 和 refresh_token
            new_access_token = security.create_access_token(data={"sub": str(user.id)})
            new_refresh_token = security.create_refresh_token(data={"sub": str(user.id)})
            
            # 更新数据库（token 轮换）
            user.refresh_token = security.create_hash(new_refresh_token)
            await self.db.commit()
                    
            # 写入 HttpOnly Cookie
            security.set_refresh_token_cookie(response, new_refresh_token)
            
            return {"access_token": new_access_token, "token_type": "bearer"}
        
        except NotFoundError:
             raise
        except AuthenticationError:
            raise
        except IntegrityError as e:
            await self.db.rollback()
            logger.error(f"IntegrityError during token refresh for user {user_id}: {e}")
            raise DatabaseError(message="Failed to refresh token due to integrity constraint")
        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"SQLAlchemyError during token refresh for user {user_id}: {e}")
            raise DatabaseError(message="Failed to refresh token due to database error")
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Unexpected error during token refresh for user {user_id}: {e}")
            raise DatabaseError(message="Failed to refresh token due to unexpected error")
        
    
    async def revoke_refresh_token(self, user: User) -> None:
        try:
            if not user.refresh_token:
                logger.info(f"No refresh token to revoke for user {user.id}")
                raise NotFoundError(resource="Refresh Token", resource_id=user.id)
            # 清除 refresh_token
            user.refresh_token = ""
            self.db.add(user)
            await self.db.commit()
            logger.info(f"Refresh token revoked for user {user.username or 'Unknown'}")
            
        except NotFoundError:
            raise
        except IntegrityError as e:
            await self.db.rollback()
            logger.error(f"IntegrityError during refresh token revocation for user {user.username or 'Unknown'}: {e}")
            raise DatabaseError(message="Failed to revoke refresh token due to integrity constraint")
        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"SQLAlchemyError during refresh token revocation for user {user.username or 'Unknown'}: {e}")    
            raise DatabaseError(message="Failed to revoke refresh token due to database error")
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Unexpected error during refresh token revocation for user {user.username or 'Unknown'}: {e}")
            raise DatabaseError(message="Failed to revoke refresh token due to unexpected error")
        
        
    async def initiate_password_reset(
        self, email: str, 
        # background_tasks: BackgroundTasks,
    ) -> bool:
        """
        发起密码重置流程
        - 查找用户（不暴露是否存在）
        - 生成密码重置令牌（有效期 15 分钟）
        - 异步发送邮件
        - 返回 True 表示流程启动成功（安全）
        """
        try:
            # 查找用户
            user = await self.user_crud.get_user_by_email(self.db, email)
            
            if not user:
                logger.info(f"Password reset requested for non-existent user: {email}")
                return True  # 即使用户不存在，也返回 True（防探测攻击）
        
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError fetching user by email {email}: {e}")
            raise DatabaseError(message="Failed to fetch user due to database error")
        except Exception as e:
            logger.error(f"Unexpected error fetching user by email {email}: {e}")
            raise DatabaseError(message="Failed to fetch user due to unexpected error")
        
        # 生成密码重置令牌（有效期 15 分钟）
        reset_token = security.create_access_token(
            data={"sub": str(user.id), "scope": "password_reset"},
            expires_delta=timedelta(minutes=15.0),
        )
        
        # 生成密码重置链接
        reset_url  = f"{settings.FRONTEND_RESET_URL}/reset-password?token={reset_token}"
        
        try:
            # 异步发送邮件：集成邮件服务发送密码重置邮件给用户
            # background_tasks.add_task(mailer.send_reset_email, email, reset_url)
            send_reset_email_task.delay(email, reset_url)
            logger.info(f"Password reset email scheduled for {email}")
            return True  
        
        except Exception as e:
            # 降级为同步发送邮件
            logger.warning(f"Celery not available, sending reset email synchronously: {e}")
            
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    mailer.send_reset_email,
                    email,
                    reset_url,
                )
                logger.info(f"Password reset email sent synchronously for {email}")
            
            except ResendError as re:
                logger.error(f"ResendError sending reset email to {email}: {re}", exc_info=True) 
            except (ConnectionError, Timeout, RequestException) as e:
                logger.error(f"Network error sending reset email to {email}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error sending reset email to {email}: {e}", exc_info=True)
                
            return True  # 即使邮件发送失败，也返回 True（防止泄露用户存在信息）
            
        
    async def reset_password_with_token(
        self,
        request: PasswordResetConfirm,
    ) -> dict[str, str]:
        """使用 token 重置密码"""
        token = request.token
        new_password = request.new_password
        
        if not token or not new_password:
            raise ValidationError(message="Missing token or password")
        
        try:
            payload = security.decode_token(token) 
             
            if payload.get("scope") != "password_reset":
                raise AuthenticationError("Invalid token scope")
        
            user_id_str = payload.get("sub")
            if not user_id_str:
                raise AuthenticationError("Token missing subject")
            user_id = UUID(user_id_str)
            
        except (ValueError, Exception):
            raise AuthenticationError("Invalid or expired token")
        
        user = await self.user_crud.get_active_user_by_id(self.db, user_id)
        if not user:
            logger.info(f"Password reset attempted for non-existent user ID: {user_id}")
            raise NotFoundError(resource="User", resource_id=user_id)
        
        await self._update_password(user, new_password)
        
        return {"message": "Password has been reset successfully"}
    
    async def _update_password(self, user: User, new_password: str) -> dict[str, str]:
        """统一密码更新逻辑"""
        try:
            user.hashed_password = security.create_hash(new_password)
            user.refresh_token = ""  # 清空 refresh_token，强制重新登录
            
            self.db.add(user)
            
            await self.db.commit()
            await self.db.refresh(user)
       
        except IntegrityError as e:
            await self.db.rollback()
            logger.warning(f"IntegrityError during password update for user {user.username}: {e}")
            raise DatabaseError("Database integrity error during password update") from e
        except SQLAlchemyError as e:
            await self.db.rollback()
            logger.error(f"Unexpected SQLAlchemyError during password update for user {user.username}: {e}")
            raise DatabaseError("Unexpected database error during password update") from e
        except Exception as e:
            await self.db.rollback()
            logger.exception(f"Unexpected error during password update for user {user.username}: {e}")
            raise DatabaseError("Unexpected error during password update") from e
    
    async def change_password_for_user(
        self,
        request: PasswordChangeRequest,
        user: User
    ) -> dict[str, str]:
        """已登录用户修改密码"""
        old_password = request.old_password
        new_password = request.new_password
        
        if not old_password or not new_password:
            raise ValidationError("Missing old or new password")
        
        if not security.verify_hash(old_password, user.hashed_password):
            logger.warning(f"Incorrect old password provided for user {user.username}")
            raise AuthenticationError("Old password is incorrect")
        
        if security.verify_hash(new_password, user.hashed_password):
            raise AuthenticationError("New password cannot be the same as the old password")
        
        await self._update_password(user, new_password)
        
        return {"message": "Password has been changed successfully"}
    