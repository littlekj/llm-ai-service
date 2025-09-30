from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from src.api.v1.api_router import api_router
from src.config.settings import settings


# 初始化日志（生产环境建议使用结构化日志，如 JSON 格式）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("knowledge_service")

# 数据库初始化（生产环境推荐使用 Alembic 迁移代替 create_all）
# Base.metadata.create_all(bind=engine)


# 创建 FastAPI 应用
app = FastAPI(title="LLM App API", version="0.1.0")
app = FastAPI(
    title="LLM App API",
    description="用于知识管理的 API 服务，提供用户管理、文档上传、问答检索等功能。",
    version="0.1.0"
)

# # 可选：定义一个简单模型
# class HealthCheck(BaseModel):
#     status: str = "healthy"
    
 
# @app.get("/")
# def read_root():
#     return {"message": "Hello, World!", "service": "llm-app"}


# @app.get("/health", response_model=HealthCheck)
# def health_check():
#     return {"status": "healthy"}


# 允许多个来源（开发 + 生产）
allow_origins = settings.ALLOWED_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,  # 生产环境要限制具体域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有请求头
)

# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error, please contact support"}
    )

# 注册相关路由
app.include_router(api_router, prefix="/api")