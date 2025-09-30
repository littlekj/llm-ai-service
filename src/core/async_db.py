from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import logging
from src.config.settings import settings

# 创建 SQLAlchemy Engine
# settings.DATABASE_URL 格式:
# mysql+pymysql://username:password@host:port/database?charset=utf8mb4
# postgresql+psycopg2://myuser:mypassword@localhost:5432/mydb

if not settings.ASYNC_DATABASE_URL:
    raise ValueError("ASYNC_DATABASE_URL is not set, please check your settings")

# 创建异步数据库引擎
async_engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,      # 数据库连接 URL 
    pool_pre_ping=True,         # 检测连接是否可用    
    pool_recycle=3600,          # 定时回收连接（避免 MySQL 8 小时断开）
    echo=False                  # True 时打印 SQL 日志，生产建议 False
)


# 创建异步会话工厂
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,  # 显示指定 AsyncSession 类型
    autoflush=False,
    autocommit=False,
    expire_on_commit=False  # 防止对象在事务提交后失效
)


@asynccontextmanager
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """异步上下文管理器，提供数据库会话"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            logging.error(f"Database session error: {e}")
            await session.rollback()
            raise
        finally:
            logging.info("Closing database session")
            await session.close()