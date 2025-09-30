from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from fastapi import HTTPException
from fastapi import BackgroundTasks
import logging

from src.schemas.user import UserCreate
from src.models.user import User
from src.core.exceptions import UserAlreadyExistsError, AuthenticationError
from src.core import security
from src.config.settings import settings
from src.utils import mailer
from src.tasks.user.reset_email import send_reset_email_task

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self, db: Session):
        self.db = db

    def create_user(self, user_in: UserCreate):
        if self.db.query(User).filter(User.username == user_in.username).first():
            raise UserAlreadyExistsError("Username")
        if self.db.query(User).filter(User.email == user_in.email).first():
            raise UserAlreadyExistsError("Email")

        hashed_pw = security.create_hash(user_in.password)
        new_user = User(
            username=user_in.username,
            email=user_in.email,
            hashed_password=hashed_pw
        )

        try:
            self.db.add(new_user)
            self.db.commit()
            self.db.refresh(new_user)
            return new_user
        except IntegrityError as e:  # 业务异常
            self.db.rollback()
            raise UserAlreadyExistsError("Username or Email")
        except SQLAlchemyError as e:  # 数据库异常
            self.db.rollback()
            logger.exception(f"DB error during registration: {e}")
            raise

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


    def refresh_token(self, raw_refresh_token: str) -> tuple[str, str, str]:
        """刷新 access_token 和 refresh_token"""
        try:
            # 解码并验证 refresh_token
            payload = security.decode_token(raw_refresh_token)
            if payload.get("type") != "refresh":
                raise HTTPException(status_code=401, detail="Invalid token type")
            
            user_id = payload.get("sub")
            jti = payload.get("jti")

            # 查询用户
            user = self.db.query(User).filter(User.id == int(user_id)).first()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")
            if not security.verify_refresh_token_in_db(raw_refresh_token, user):
                raise HTTPException(status_code=401, detail="Invalid or revoked refresh token")
            
            # 生成新的 access_token 和 refresh_token
            new_access_token = security.create_access_token(data={"sub": user.id})
            new_fresh_token = security.create_refresh_token(data={"sub": user.id})
            
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
        
        user_id = int(payload.get("sub"))
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
