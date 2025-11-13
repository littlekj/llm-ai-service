import logging
import logging.config
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from src.api.v1.api_router import api_router
from src.config.settings import settings
from src.config.logging import logging_config
from src.middleware.request_id import RequestIDMiddleware
from src.middleware.request_id import setup_custom_log_record_factory
from src.middleware.request_id import request_id_ctx_var
from src.core.exception_handlers import register_exception_handlers


# 在任何日志配置之前安装 LogRecordFactory
setup_custom_log_record_factory()

# 配置根日志器（生产环境使用结构化日志，如 JSON 格式）
LOG_LEVEL = settings.APP_LOG_LEVEL or logging.INFO
logging_config["root"]["level"] = LOG_LEVEL
logging.config.dictConfig(logging_config)

# 添加logger定义
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用
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




# 注册中间件

# 允许多个来源（开发 + 生产）
allow_origins = settings.ALLOWED_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,  # 生产环境要限制具体域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有请求头
)

app.add_middleware(RequestIDMiddleware)

# 注册全局异常处理器
register_exception_handlers(app=app)


# # 将 RequestValidationError 错误详情打印到日志
# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request: Request, exc: RequestValidationError):
#     rid = request_id_ctx_var.get()
#     body_bytes = await request.body()
#     body_text = body_bytes.decode(errors="replace")
#     logger.error(
#         f"[request_id={rid}] Request validation error: {request.method} {request.url} body={body_text} errors={exc.errors()}"
#     )
#     return JSONResponse(status_code=422, content={"detail": exc.errors()})

# 注册路由
app.include_router(api_router, prefix="/api")

