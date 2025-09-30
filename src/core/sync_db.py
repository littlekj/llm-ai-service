from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager

import logging
from src.config.settings import settings

# 创建 SQLAlchemy Engine
# settings.DATABASE_URL 格式:
# mysql+pymysql://username:password@host:port/database?charset=utf8mb4
# postgresql+psycopg2://myuser:mypassword@localhost:5432/mydb

if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL is not set, please check your settings")

# 创建数据库引擎
engine = create_engine(
    settings.DATABASE_URL,      # 数据库连接 URL 
    pool_pre_ping=True,         # 检测连接是否可用    
    pool_recycle=3600,          # 定时回收连接（避免 MySQL 8 小时断开）
    echo=False                  # True 时打印 SQL 日志，生产建议 False
)

# 创建会话工厂
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


@contextmanager
def get_sync_db():
    """提供数据库会话"""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        logging.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        logging.info("Closing database session")
        db.close()