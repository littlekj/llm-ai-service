from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    """应用核心配置"""
    
    # 基础配置
    DEBUG: bool = False
    ENVIRONMENT: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DOMAIN: str = "localhost"
    PROJECT_NAME: str = "LLM AI Service"

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
    
    # Celery 配置
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

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
