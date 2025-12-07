import logging
from sqlalchemy import select, update, func, delete, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from uuid import UUID
from typing import Optional
from fastapi import Depends

from src.models.user import User
from src.schemas.user import UserCreate
from src.core.database import get_async_db


class UserCRUD:
    
    async def check_existing_user(
        self,
        db: AsyncSession,
        email: str,
        username: str
    ) -> tuple[Optional[User], Optional[User], Optional[User]]:
        """
        检查所有可能冲突的用户
        :param email: 用户邮箱
        :param username: 用户名
        :return: active_user, inactive_user, deleted_user
        """
        stmt = select(User).where(
            and_(
                User.username == username,
                User.email == email
            )
        )
        result = await db.execute(stmt)
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
    
    async def create_user_async(
        self,
        db: AsyncSession,
        user_in: UserCreate,
        hashed_pw: str,
    ):
        new_user = User(
            username=user_in.username,
            email=user_in.email,
            hashed_password=hashed_pw,
            is_active=False,
        )
    
        db.add(new_user)
        await db.flush()
        await db.refresh(new_user)
        
        return new_user
        
    async def get_active_user_by_id(
        self,
        db: AsyncSession,
        user_id: UUID,
    ) -> Optional[User]:
        stmt = select(User).where(
            User.id == user_id,
            User.is_active == True,
        )
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()
        
        return user
    
    async def get_user(
        self,
        db: AsyncSession,
        username_or_email: str,
    ) -> Optional[User]:
        """通过用户名或邮箱获取用户"""        
        stmt = select(User).where(
            or_(
                User.username == username_or_email,
                User.email == username_or_email
            ),
        ) 

        result = await db.execute(stmt)
        user = result.scalar_one_or_none()
        
        return user
    
    async def get_active_user(
        self,
        db: AsyncSession,
        username_or_email: str,
    ) -> Optional[User]:
        """通过用户名或邮箱获取用户"""        
        stmt = select(User).where(
            or_(
                User.username == username_or_email,
                User.email == username_or_email
            ),
            User.is_active == True,
        ) 

        result = await db.execute(stmt)
        user = result.scalar_one_or_none()
        
        return user
    
    async def get_user_by_email(
        self,
        db: AsyncSession,
        email: str,
    ):
        stmt = select(User).where(User.email == email)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        return user
        
    async def active_user_async(
        self,
        db: AsyncSession,
        user_id: UUID,
    ):
        stmt = update(User).where(
            User.id == user_id,
            User.is_active == False,
        ).values(
            is_active=True,
            email_confirmed_at = func.now(),
            is_deleted=False,
            deleted_at=None,
        )
        
        result = await db.execute(stmt)
        await db.flush()
        
        if result.rowcount == 0:
            return False
            
        return True

    async def update_token_stats_async(
        self,
        db: AsyncSession,
        user_id: UUID,
        used_tokens: int,
    ) -> bool:
        """更新用户的 token 使用统计"""
        stmt = (
            update(User)
            .where(
                User.id == user_id,
                User.quota_tokens >= used_tokens,
            )
            .values(
                quota_tokens=User.quota_tokens - used_tokens,
                used_tokens=User.used_tokens + used_tokens,
            )
            .returning(User.id)  # 返回更新的用户 ID 即可判断是否有行被更新
        )
        result = await db.execute(stmt)
        update_id = result.scalar_one_or_none()
        await db.flush()
        
        if update_id is None:
            return False
            
        return True
