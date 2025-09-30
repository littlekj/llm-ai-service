from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import contextmanager, asynccontextmanager
from typing import Generator, AsyncGenerator

import logging
from src.config.settings import settings

# 创建 SQLAlchemy Engine
# settings.DATABASE_URL 格式:
# mysql+pymysql://username:password@host:port/database?charset=utf8mb4
# postgresql+psycopg2://myuser:mypassword@localhost:5432/mydb

if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL is not set, please check your settings")

# 创建同步引擎
engine = create_engine(
    settings.DATABASE_URL,      # 数据库连接 URL
    pool_pre_ping=True,         # 检测连接是否可用
    pool_recycle=3600,          # 定时回收连接（避免 MySQL 8 小时断开）
    echo=False                  # True 时打印 SQL 日志，生产建议 False
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

if not settings.ASYNC_DATABASE_URL:
    raise ValueError(
        "ASYNC_DATABASE_URL is not set, please check your settings")

# 创建异步引擎
async_engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,      # 数据库连接 URL
    pool_pre_ping=True,         # 检测连接是否可用
    pool_recycle=3600,          # 定时回收连接（避免 MySQL 8 小时断开）
    echo=False                  # True 时打印 SQL 日志，生产建议 False
)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,  # 显示指定 AsyncSession 类型
    autoflush=False,
    autocommit=False,
    expire_on_commit=False  # 防止对象在事务提交后失效
)


# 创建数据库会话
@contextmanager
def get_sync_db():
    """提供同步数据库会话"""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logging.error(f"Database session error: {e}")
        raise
    finally:
        logging.info("Closing database session")
        db.close()

@asynccontextmanager
async def get_async_db():
    """提供异步数据库会话"""
    db = AsyncSessionLocal()
    try:
        yield db
        await db.commit()
    except Exception as e:
        await db.rollback()
        logging.error(f"Async Database session error: {e}")
        raise
    finally:
        logging.info("Closing Async database session")
        await db.close()
