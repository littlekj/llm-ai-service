from fastapi import Request, status, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from minio.error import S3Error
from typing import Union
import logging

from src.core.exceptions import BaseAppException
from src.config.settings import settings
from src.middleware.request_id import request_id_ctx_var

logger = logging.getLogger(__name__)


def sanitize_error_for_production(error: str, exc_type: str) -> str:
    """生产环境下过滤敏感信息"""
    if settings.ENVIRONMENT == "production":
        # 移除文件路径
        import re
        error = re.sub(r'[A-Za-z]:\\[^\s]+', '[PATH_REDACTED]', error)
        error = re.sub(r'/[^\s]+\.py', '[FILE_REDACTED]', error)
        # 移除敏感关键词
        for keyword in ['password', 'secret', 'token', 'key']:
            error = re.sub(rf'{keyword}[=:]\s*\S+', f'{keyword}=[REDACTED]', error, flags=re.IGNORECASE)
        return f"Internal error occurred ({exc_type})"
    return error


async def base_app_exception_handler(request: Request, exc: BaseAppException):
    """处理自定义业务异常"""
    request_id = request_id_ctx_var.get()
    
    # LogRecord extra 中无需手动传递 request_id
    # ContextFilter 会自动从 ContextVar 获取并注入到 LogRecord
    logger.error(
        f"Business exception: {exc.error_code} - {exc.message}",
        extra={
            # "request_id": request_id,      # 请求ID
            "error_code": exc.error_code,  # 业务错误码
            "path": str(request.url),      # 请求路径
            "method": request.method,      # HTTP方法
            "details": exc.details,        # 异常详情
        },
        exc_info=exc.status_code >= 500
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                # "request_id": request_id,
                "error_code": exc.error_code,
                "message": exc.message,
                "details": exc.details if settings.ENVIRONMENT != "production" else {},
            }
        }
    )

async def http_exception_passthrough_handler(request: Request, exc: HTTPException):
    """直接透传FastAPI/Starlette 原生抛出的 HTTPException"""
    request_id = request_id_ctx_var.get()
    
    logger.warning(
        f"HTTP exception: {exc.status_code} - {exc.detail}",
        extra={
            # "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
        }
    )
    
    # 尝试解析结构化信息
    error_code = "http_error"
    message = str(exc.detail)
    detail = None
    if isinstance(exc.detail, dict):
        # 若 detail 本身包含 error_code，则透传；否则给个通用编码
        error_code = exc.detail.get("error_code", "http_error")
        message =exc.detail.get("message", str(exc.detail))
        detail = exc.detail.get("detail")
        
    detail = exc.detail if isinstance(exc.detail, (str, dict)) else str(exc.detail)
    
    content = {
        "error": {
            # "request_id": request_id,
            "error_code": error_code,
            "message": message,
            "detail": detail,
        }
    }
    return JSONResponse(status_code=exc.status_code, content=content)


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """处理 Pydantic 参数校验失败（422）"""
    request_id = request_id_ctx_var.get()
    errors = exc.errors()
    
    logger.warning(
        f"Request validation failed: {len(errors)} error(s)",
        extra={
            # "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
        }
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                # "request_id": request_id,
                "error_code": "validation_error",
                "message": "Request validation failed",
                "details": errors,  # 验证错误哦返回给前端调试
            }
        }
    )

async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
    """处理数据库异常"""
    request_id = request_id_ctx_var.get()
    
    # 区分不同类型的数据库错误
    if isinstance(exc, IntegrityError):
        logger.warning(
            f"Database integrity error: {str(exc)}", exc_info=True,
            extra={
                # "request_id": request_id,
                "path": str(request.url),
            }
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": {
                    # "request_id": request_id,
                    "error_code": "resource_conflict",
                    "message": "Resource already exists or violates constraints",
                }
                    
            }
        )
    elif isinstance(exc, OperationalError):
        logger.error(f"Database operational error: {exc}", exc_info=True, extra={"request_id": request_id})
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": {
                    # "request_id": request_id,
                    "error_code": "database_unavailable",
                    "message": "Database service temporarily unavailable",
                }
            }
        )
    # 其他数据库错误
    logger.error(f"Unexpected database error: {type(exc).__name__}", exc_info=True, extra={"request_id": request_id})
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                # "request_id": request_id,
                "error_code": "database_error",
                "message": "Unexpected database error occurred ",
            }
        }
    )

async def s3_exception_handler(request: Request, exc: S3Error):
    """处理 MinIO/S3 异常"""
    request_id = request_id_ctx_var.get()
    
    logger.error(
        f"S3 error: {exc.code} - {exc.message}",
        extra={
            # "request_id": request_id,
            "s3_code": exc.code,
            "path": str(request.url),
        },
        exc_info=True
    )
    
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": {
                # "request_id": request_id,
                "error_code": "storage_service_error",
                "message": "Storage service temporarily unavailable",
                "detail": {"s3_code": exc.code} if settings.ENVIRONMENT != "production" else None,
            }
        }
    )

async def global_exception_handler(request: Request, exc: Exception):
    """捕获所有未处理的异常（500）"""
    request_id = request_id_ctx_var.get()
    exc_type = type(exc).__name__
    
    logger.error(
        f"Unhandled exception: {exc_type} - {str(exc)}",
        extra={
            # "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "user_agent": request.headers.get("user-agent"),
        },
        exc_info=True
    )
    
    # 根据环境返回不同的错误信息
    error_message = sanitize_error_for_production(str(exc), exc_type)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                # "request_id": request_id,
                "error_code": "internal_server_error",
                "message": error_message,
            }
        }
    )

# 注册入口
def register_exception_handlers(app):
    """注册所有异常处理器"""
    from minio.error import S3Error
    # 先注册具体的异常处理器，再注册全局异常处理器
    app.add_exception_handler(BaseAppException, base_app_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_passthrough_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(SQLAlchemyError, sqlalchemy_exception_handler)
    app.add_exception_handler(S3Error, s3_exception_handler)
    app.add_exception_handler(Exception, global_exception_handler)