from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    """应用核心配置"""
    
    # 基础配置
    DEBUG: bool = False
    ENVIRONMENT: str = "development"  # development | production | staging
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DOMAIN: str = "localhost"
    PROJECT_NAME: str = "llm-ai-service"

    # 数据库配置
    # 方式1：优先使用完整连接串（云服务友好）
    DATABASE_URL: Optional[str] = None
    SYNC_DATADASE_URL: Optional[str] = None
    ASYNC_DATABASE_URL: Optional[str] = None
    
    # 方式2：本地开发用拼接
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: SecretStr = SecretStr("")
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""
    
    # MinIO 配置
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ROOT_USER: str = ""
    MINIO_ROOT_PASSWORD: str = ""
    MINIO_SECURE: bool = False
    MINIO_BUCKET_NAME: str = "documents"

    @property
    def SQLALCHEMY_DATABASE_URL(self) -> str:
        """返回 SQLAlchemy 使用的数据库连接字符串"""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        
        pwd = self.POSTGRES_PASSWORD.get_secret_value()
        # return f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?charset=utf8mb4"
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{pwd}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    def SQLALCHEMY_ASYNC_DATABASE_URL(self) -> str:
        """返回 SQLAlchemy 使用的数据库连接字符串"""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        
        pwd = self.POSTGRES_PASSWORD.get_secret_value()
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{pwd}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # JWT 配置
    JWT_SECRET_KEY: SecretStr = SecretStr("")
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15   # 短期
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7      # 长期

    # 邮件服务
    RESEND_API_KEY: SecretStr = SecretStr("")
    RESEND_FROM_EMAIL: str = ""
    FRONTEND_RESET_URL: str = ""
    # 前端邮箱确认页面地址（用于发送确认邮件）
    FRONTEND_CONFIRM_URL: str = ""
    
    # Redis 配置
    REDIS_HOST: Optional[str] = None
    REDIS_PORT: Optional[int] = None
    REDIS_DB: Optional[int] = None
    REDIS_PASSWORD: SecretStr = SecretStr("")
    
    # Celery 配置
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"
    CELERY_WORKER_CONCURRENCY: Optional[int] = None
    
    # 日志配置
    APP_LOG_LEVEL: str = "INFO"

    # 文档保留策略
    DOCUMENT_RETENTION_PERIOD_DAYS: float = 30.0
    
    # Qdrant 配置
    QDRANT_SERVER_URL: str = "http://localhost:6333"
    QDRANT_API_KEY: SecretStr = SecretStr("")
    
    # Embeddings 配置
    EMBEDDING_MODEL_NAME: str = ""
    EMBEDDING_MODEL_CACHE_DIR: Optional[str] = None
    
    # LLM 配置
    LLM_MODEL_NAME: str = ""
    LLM_API_URL: str = ""
    LLM_API_KEY: SecretStr = SecretStr("")
    
    # SSL 配置
    SSL_CERT_FILE: Optional[str] = None

    # CORS 配置
    ALLOWED_ORIGINS: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:8001",
            "http://127.0.0.1:8001",
            "http://localhost:8002",
            "http://127.0.0.1:8002",
        ]
    )
    
    def model_post_init(self, __context) -> None:
        if not self.DOMAIN:
            return
        clean_domain = self.DOMAIN.strip().rstrip("/")
        if clean_domain.startswith(("http://", "https://")):
            origin = clean_domain
        else:
            origin = f"https://{clean_domain}"
        if origin not in self.ALLOWED_ORIGINS:
            self.ALLOWED_ORIGINS.append(origin)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

@lru_cache()
def get_settings() -> Settings:
    """
    返回缓存的 Settings 实例
    使用 lru_cache 缓存实例，避免重复初始化
    """
    return Settings()


# 全局唯一实例
settings = get_settings()
# print("setting.DATABASE_URL:", settings.DATABASE_URL)
