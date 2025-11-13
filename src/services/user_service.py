import logging
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from fastapi import HTTPException
from fastapi import BackgroundTasks
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from uuid import UUID
from jwt import ExpiredSignatureError, InvalidTokenError

from src.schemas.user import UserCreate
from src.models.user import User
from src.crud.user import UserCRUD, get_user_dao
from src.core.exceptions import UserAlreadyExistsError, AuthenticationError
from src.core import security
from src.config.settings import settings
from src.utils import mailer
from src.workers.user.email_notification import send_confirmation_email_task, send_reset_email_task

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self, db):
        self.db = db
        
    async def check_existing_user(
        self,
        email: str,
        usrename: str
    ) -> tuple[Optional[User], Optional[User], Optional[User]]:
        """
        检查所有可能冲突的用户
        :param email: 用户邮箱
        :param usrename: 用户名
        :return: active_user, inactive_user, deleted_user
        """
        stmt = select(User).where(
            or_(
                User.username == usrename,
                User.email == email
            )
        )
        result = await self.db.execute(stmt)
        users = result.scalars().all()

        active_user = None
        inactive_user = None
        deleted_user = None
        
        for user in users:
            if user.is_deleted:
                deleted_user = user
            elif user.is_active:
                active_user = user
            else:
                inactive_user = user
                
        return active_user, inactive_user, deleted_user

    async def register_user(self, user_in: UserCreate):
        """
        注册新用户，处理用户名/邮箱冲突和未激活账户
        """
        # 检查是否存在冲突的用户
        active_user, inactive_user, deleted_user = await self.check_existing_user(user_in.email, user_in.username)
        
        if active_user:
            # 用户名或邮箱已注册
            if active_user.email == user_in.email:
                raise HTTPException(status_code=409, detail="Email already registered")
            else:
                raise HTTPException(status_code=409, detail="Username already taken")
        
        if deleted_user:
            # 删除账户后30天内不允许重新注册
            threshold = datetime.now(timezone.utc) - timedelta(days=30)
            if deleted_user.deleted_at and deleted_user.deleted_at > threshold:
                raise HTTPException(status_code=409, detail="Account deleted within 30 days")
                
        if inactive_user:
            # 未激活账户7天内允许重新激活
            logger.info(f"Found inactive user: {inactive_user.email}")
            threshold = datetime.now(timezone.utc) - timedelta(days=7)
            if inactive_user.created_at < threshold:
                raise HTTPException(status_code=409, detail="Account not activated for more than 7 days")
            
            else:
                # 重新激活账户
                hashed_pw = security.create_hash(user_in.password)
                user_crud = UserCRUD()
                try:
                    updated_user = await user_crud.update_inactive_user_async(
                        self.db, 
                        inactive_user.id,
                        user_in,
                        hashed_pw
                    )
                    if not updated_user:
                        raise HTTPException(status_code=409, detail="Failed to update inactive account")
                    logger.info(f"Reactivating inactive user: {updated_user.hashed_password}")
                    await self._send_activation_email(updated_user)
                    return updated_user

                except Exception as e:
                    logger.exception(f"Failed to update inactive account: {e}", exc_info=True)
                    raise  
    
        # 创建新用户
        try:
            logger.info(f"Found new user: {user_in.email}")
            # 密码哈希处理
            hashed_pw = security.create_hash(user_in.password)
            # 新用户注册后暂不激活，需要邮箱确认
            user_crud = UserCRUD()
            new_user = await user_crud.create_user_async(
                self.db,
                user_in,
                hashed_pw,
            )
            
            # 发送用户注册确认邮件
            await self._send_activation_email(new_user)
            return new_user
        
        except Exception as e:
            logger.exception(f"Failed to schedule/send confirmation email: {e}")
            raise
    
    async def confirm_email(self, token: str) -> dict[str, str]:
        """确认用户邮箱并激活账户"""
        try:
            payload = security.decode_token(token)
        except ExpiredSignatureError:
            raise ValueError("Expired token")
        except InvalidTokenError:
            raise ValueError("Invalid token")
        except Exception:
            raise ValueError("Unexpected error while decoding token")
        
        if payload.get("scope") != "email_confirm":
            raise ValueError("Invalid token scope")
        
        user_id = UUID(payload.get("sub"))
        user_crud = UserCRUD()
        
        try:
            await user_crud.active_user_async(self.db, user_id)
            return {"message": "Email confirmed"}
        except Exception as e:
            logger.exception(f"Failed to activate user {user_id}: {e}")
            raise RuntimeError("Failed to activate user")
        
    async def resend_confirmation_email(self, email: str):
        """重新发送邮箱确认邮件"""   
        user_crud = UserCRUD()
        user = await user_crud.get_user_by_email(self.db, email)
             
        # user = self.db.query(User).filter(User.email == email).first()
        if not user:
            raise ValueError("User not found")

        if user.is_active and user.email_confirmed_at:
            raise ValueError("Email already confirmed")
        
        try:
            await self._send_activation_email(user)
            return {"message": "Confirmation email sent"}
        except Exception as e:
            logger.exception(f"Failed to resend confirmation email: {e}")
            raise
            
    async def _send_activation_email(self, new_user: User):
        """生成邮件确认 token（scope=email_confirm）并发送确认邮件"""
        try:
            confirm_token = security.create_access_token(
                data={"sub": str(new_user.id), "scope": "email_confirm"},
                expires_delta=timedelta(minutes=15.0)
            )
            confirm_url = f"{settings.FRONTEND_CONFIRM_URL}/confirm-email?token={confirm_token}"
                
            try:
                # 使用 celery 发送邮件（如果可用）
                send_confirmation_email_task.delay(new_user.email, confirm_url)
                logger.info(f"Scheduled confirmation email for {new_user.email}")
            except Exception:
                # 回退到直接发送（同步）
                logger.warning("Celery not available, sending confirmation email synchronously")
                mailer.send_confirmation_email(new_user.email, confirm_url)
        except Exception as e:
            logger.exception(f"Failed to schedule/send confirmation email: {e}")


    def authenticate(self, username: str, password: str) -> User:
        # 支持用户名或邮箱登录
        user = self.db.query(User).filter(
            (User.username == username) |
            (User.email == username)
        ).first()
        
        if not user:
            raise AuthenticationError("Invalid credentials")
        if not user.is_active:
            raise AuthenticationError("User account is disabled")
        if not security.verify_hash(password, user.hashed_password):
            raise AuthenticationError("Invalid credentials")
        return user


    def refresh_token(self, raw_refresh_token: str) -> tuple[str, str, UUID]:
        """刷新 access_token 和 refresh_token"""
        try:
            # 解码并验证 refresh_token
            payload = security.decode_token(raw_refresh_token)
            if payload.get("type") != "refresh":
                raise HTTPException(status_code=401, detail="Invalid token type")
            
            user_id: UUID = UUID(payload.get("sub"))
            jti = payload.get("jti")

            # 查询用户
            user = self.db.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")
            if not security.verify_refresh_token_in_db(raw_refresh_token, user):
                raise HTTPException(status_code=401, detail="Invalid or revoked refresh token")
            
            # 生成新的 access_token 和 refresh_token
            new_access_token = security.create_access_token(data={"sub": str(user.id)})
            new_fresh_token = security.create_refresh_token(data={"sub": str(user.id)})
            
            # 更新数据库（token 轮换）
            user.refresh_token = security.create_hash(new_fresh_token)
            self.db.add(user)  # 显式添加到会话
            self.db.commit()   # 提交更改
            
            return new_access_token, new_fresh_token, user_id
        
        except HTTPException:
             raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Unexpected error during token refresh: {e}")
            raise HTTPException(status_code=500, detail="Failed to refresh token")
        
    
    def revoke_refresh_token(self, user: User) -> None:
        try:
            if user.refresh_token:
                # 清除 refresh_token
                user.refresh_token = ""
                self.db.add(user)
                self.db.commit()
                logger.info(f"Refresh token revoked for user {user.username}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to revoken refresh token for user {user.username}: {e}")
            raise
        
        
    def initiate_password_reset(self, email: str, background_tasks: BackgroundTasks) -> bool:
        """
        发起密码重置流程
        - 查找用户（不暴露是否存在）
        - 生成密码重置令牌（有效期 15 分钟）
        - 异步发送邮件
        - 返回 True 表示流程启动成功（安全）
        """
        user = self.db.query(User).filter(User.email == email).first()
        if not user:
            # logger.info(f"Password reset requested for non-existent email: {email}")
            return True  # 即使用户不存在，也返回 True（防探测攻击）

        try:
            # 生成密码重置令牌（有效期 15 分钟）
            reset_token = security.create_access_token(
                data={"sub": str(user.id), "scope": "password_reset"},
            )
            # 异步发送邮件
            # 集成邮件服务发送密码重置邮件给用户
            reset_url  = f"{settings.FRONTEND_RESET_URL}/reset-password?token={reset_token}"
            # background_tasks.add_task(mailer.send_reset_email, email, reset_url)
            send_reset_email_task.delay(email, reset_url)
            logger.info(f"Password reset email scheduled for {email}")
            return True  
        except Exception as e:
            logger.error(f"Failed to schedule password reset email for {email}: {e}")   
            return True  # 即使邮件发送失败，也返回 True（防止泄露用户存在信息）
        
    def reset_password_with_token(self, token: str, new_password: str) -> dict[str, str]:
        """使用 token 重置密码"""
        try:
            payload = security.decode_token(token)  
        except Exception:
            raise ValueError("Invalid or expired token")
        
        if payload.get("scope") != "password_reset":
            raise ValueError("Invalid token scope")
        
        user_id = UUID(payload.get("sub"))
        user = self.db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise ValueError("User not found")
        
        return self._update_password(user, new_password)
    
    def change_password_for_user(self, user: User, old_password: str, new_password: str) -> dict[str, str]:
        """已登录用户修改密码"""
        if not security.verify_hash(old_password, user.hashed_password):
            raise ValueError("Old password is incorrect")
        
        if security.verify_hash(new_password, user.hashed_password):
            raise ValueError("New password cannot be the same as the old password")
        
        return self._update_password(user, new_password)
    
    def _update_password(self, user: User, new_password: str) -> dict[str, str]:
        """统一密码更新逻辑"""
        try:
            user.hashed_password = security.create_hash(new_password)
            user.refresh_token = ""  # 清空 refresh_token，强制重新登录
            self.db.add(user)
            self.db.commit()
            self.db.refresh(user)  # 刷新对象，确保后续使用的是最新数据
            return {"message": "Password updated successfully"}
       
        # 注意：基类异常必须放在子类异常的后面
        except IntegrityError as e:
            self.db.rollback()
            logger.warning(f"IntegrityError during password update for user {user.username}: {e}")
            raise ValueError("Password update failed due to integrity constraint")
        
        except OperationalError as e:
            self.db.rollback()
            logger.error(f"OperationalError during password update for user {user.username}: {e}")
            raise RuntimeError("Database operation failed during password update")
        
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Unexpected SQLAlchemyError during password update for user {user.username}: {e}")
            raise RuntimeError("Unexpected database error during password update") from e
