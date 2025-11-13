from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from minio.error import S3Error
from typing import Union
import logging

from src.core.errors import BaseAppException, ErrorCode
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
    request_id_ctx_var.get()
    
    logger.error(
        f"Business exception: {exc.error_code} - {exc.message}",
        extra={
            "error_code": exc.error_code,  # 业务错误码
            "path": str(request.url),      # 请求路径
            "method": request.method,      # HTTP方法
            "details": exc.details,        # 异常详情
        },
        exc_info=settings.ENVIRONMENT != "production"
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error_code": exc.error_code,
            "message": exc.message,
            "details": exc.details if settings.ENVIRONMENT != "production" else {},
        }
    )


async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
    """处理数据库异常"""
    request_id = request_id_ctx_var.get()
    
    # 区分不同类型的数据库错误
    if isinstance(exc, IntegrityError):
        logger.warning(
            f"Database integrity error: {str(exc)}",
            extra={"request_id": request_id, "path": str(request.url)}
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error_code": ErrorCode.DUPLICATE_ENTRY,
                "message": "Resource already exists",
            }
        )
    
    elif isinstance(exc, OperationalError):
        logger.error(
            f"Database connection error: {str(exc)}",
            extra={"request_id": request_id},
            exc_info=True
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error_code": ErrorCode.DATABASE_ERROR,
                "message": "Database temporarily unavailable",
            }
        )
    
    # 其他数据库错误
    logger.error(
        f"Unexpected database error: {type(exc).__name__}",
        extra={"request_id": request_id},
        exc_info=True
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error_code": ErrorCode.DATABASE_ERROR,
            "message": "Database error occurred",
        }
    )


async def s3_exception_handler(request: Request, exc: S3Error):
    """处理 MinIO/S3 异常"""
    request_id = request_id_ctx_var.get()
    
    logger.error(
        f"S3 error: {exc.code} - {exc.message}",
        extra={
            "s3_code": exc.code,
            "path": str(request.url),
        },
        exc_info=True
    )
    
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error_code": ErrorCode.STORAGE_SERVICE_ERROR,
            "message": "Storage service error",
        }
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """处理请求验证异常"""
    request_id = request_id_ctx_var.get()
    
    errors = exc.errors()
    logger.warning(
        f"Request validation failed: {len(errors)} error(s)",
        extra={
            "validation_errors": errors,
            "path": str(request.url),
        }
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": "Request validation failed",
            "details": errors,
        }
    )


async def global_exception_handler(request: Request, exc: Exception):
    """捕获所有未处理的异常"""
    request_id = request_id_ctx_var.get()
    exc_type = type(exc).__name__
    
    logger.error(
        f"Unhandled exception: {exc_type}",
        extra={
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
            "error_code": ErrorCode.INTERNAL_ERROR,
            "message": error_message,
        }
    )


def register_exception_handlers(app):
    """注册所有异常处理器"""
    from minio.error import S3Error
    
    app.add_exception_handler(BaseAppException, base_app_exception_handler)
    app.add_exception_handler(SQLAlchemyError, sqlalchemy_exception_handler)
    app.add_exception_handler(S3Error, s3_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, global_exception_handler)