
from __future__ import annotations  # 开启延迟解析，避免循环导入问题
from sqlalchemy import Integer, String, Boolean, func, Index
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime, timezone
from enum import Enum
from typing import List
from src.models import Base


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .document import Document
    
    
class UserRole(str, Enum):
    """用户角色枚举"""
    ADMIN = "admin"
    USER = "user"
    GUEST = "guest"


class User(Base):

    """
    User类，用于表示系统中的用户实体
    继承自Base类，使用SQLAlchemy ORM映射到数据库表
    """
    __tablename__ = "users"

    # ORM 2.0 版本数据库字段定义
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)  # 用户ID，主键，建立索引
    username: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)  # 用户名，唯一且不可为空
    email: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)  # 电子邮箱，唯一且不可为空
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)  # 哈希后的密码，不可为空
    
    role: Mapped[str] = mapped_column(String(20), default=UserRole.USER, nullable=False)  # 用户角色，默认为"user"
    quota_tokens: Mapped[int] = mapped_column(Integer, default=10000, nullable=False)  # 用户配额，默认为10000
    used_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)  # 用户已使用配额，默认为0
    refresh_token: Mapped[str] = mapped_column(String(1024), nullable=True)  # 存放最新的 Refresh Token
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)  # 用户是否激活，默认为True
    
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), 
        server_default=func.now(),  # 数据库层生成 UTC 时间
        nullable=False
    )
    
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),  
        nullable=False
    )
    
    deleted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True  # 软删除时间
    )

    # 一对多关系：User -> Document
    documents: Mapped[List["Document"]] = relationship(
        "Document",  # 关联的类名
        back_populates="user",  # 反向引用属性名
        cascade="all, delete-orphan",  # 级联删除其关联的文档
        passive_deletes=True  # 激活被动删除
    )
    
    # 命名规范：ix_<表名>_<字段1>_<字段2>
    __table_args__ = (
        Index("ix_users_email_active", "email", "is_active"),  # 登录常用查询
        Index("ix_users_deleted_at", "deleted_at"),  # 软删除查询
    )
    
    def __repr__(self) -> str:
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}', role='{self.role}')>"
