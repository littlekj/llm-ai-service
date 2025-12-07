import logging
import logging.config
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from src.api.v1.api_router import api_router
from src.config.settings import settings
from src.config.logging import logging_config, setup_log_record_factory
from src.middleware.request_id import RequestIDMiddleware
from src.middleware.request_id import request_id_ctx_var
from src.core.exception_handlers import register_exception_handlers


# 首先安装 LogRecordFactory
setup_log_record_factory()

# 配置根日志器（生产环境使用结构化日志，如 JSON 格式）
LOG_LEVEL = settings.APP_LOG_LEVEL or logging.INFO
logging_config["root"]["level"] = LOG_LEVEL
logging.config.dictConfig(logging_config)

# 获取日志记录器
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用
app = FastAPI(
    title="LLM App API",
    description="用于知识管理的 API 服务，提供用户管理、文档上传、问答检索等功能。",
    version="0.1.0"
)

# 配置 CORS 中间件
# 允许多个来源（开发 + 生产）
allow_origins = settings.ALLOWED_ORIGINS
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,  # 生产环境要限制具体域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有请求头
)

# 添加请求ID中间件
app.add_middleware(RequestIDMiddleware)

# 注册全局异常处理器
register_exception_handlers(app=app)

# 注册路由
app.include_router(api_router, prefix="/api")
