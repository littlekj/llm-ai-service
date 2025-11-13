import logging
from sqlalchemy import select, update, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from uuid import UUID
from typing import Optional
from fastapi import Depends

from src.models.user import User
from src.schemas.user import UserCreate
from src.core.database import get_async_db


class UserCRUD:
    
    async def create_user_async(
        self,
        db: AsyncSession,
        user_in: UserCreate,
        hashed_pw: str,
    ):
        try:
            new_user = User(
                username=user_in.username,
                email=user_in.email,
                hashed_password=hashed_pw,
                is_active=False,
            )
        
            db.add(new_user)
            await db.commit()
            await db.refresh(new_user)
            
            return new_user
        
        except SQLAlchemyError as e:
            await db.rollback()
            logging.error(f"Database error creating user: {str(e)}", exc_info=True)
            raise
        
    async def update_inactive_user_async(
        self,
        db: AsyncSession,
        user_id: UUID,
        user_in: UserCreate,
        hashed_pw: str,
    ):
        try:
            stmt = select(User).where(
                User.id == user_id,
            )
            result = await db.execute(stmt)
            user = result.scalars().first()
            if user is None:
                return None
            
            user.username = user_in.username
            user.email = user_in.email
            user.hashed_password = hashed_pw
            user.is_active = False
            
            await db.commit()
            await db.refresh(user)
            
            return user 
            
        except SQLAlchemyError as e:
            await db.rollback()
            logging.error(f"Database error updating user: {str(e)}", exc_info=True)
            raise
        
    async def active_user_async(
        self,
        db: AsyncSession,
        user_id: UUID,
    ):
        try:
            stmt = update(User).where(
                User.id == user_id,
                User.is_active == False,
            ).values(
                is_active=True,
                email_confirmed_at = func.now()  
            )
            
            await db.execute(stmt)
            await db.commit()

        except SQLAlchemyError as e:
            await db.rollback()
            logging.error(f"Database error updating user: {str(e)}", exc_info=True)
            raise
        
    async def get_user_by_email(
        self,
        db: AsyncSession,
        email: str,
    ):
        try:
            stmt = select(User).where(User.email == email)
            result = await db.execute(stmt)
            user = result.scalars().first()

            return user

        except SQLAlchemyError as e:
            logging.error(f"Database error getting user by email: {str(e)}", exc_info=True)
            raise
    
    async def update_token_stats_async(
        self,
        db: AsyncSession,
        user_id: UUID,
        used_tokens: int,
    ) -> bool:
        """更新用户的 token 使用统计"""
        try:
            stmt = (
                update(User)
                .where(User.id == user_id)
                .values(
                    quota_tokens=User.quota_tokens - used_tokens,
                    used_tokens=User.used_tokens + used_tokens,
                )
                .returning(User)
            )
            result = await db.execute(stmt)
            await db.commit()
            return True
        
        except SQLAlchemyError as e:
            await db.rollback()
            logging.error(f"Database error updating token stats: {str(e)}", exc_info=True)
            raise
        
        except Exception as e:
            await db.rollback()
            logging.error(f"Unexpected error updating token stats: {str(e)}", exc_info=True)
            raise
        
async def get_user_dao():
    return UserCRUD()