from enum import Enum
from typing import Optional, Dict, Any
from fastapi import HTTPException, status


# 业务错误码定义及异常类

class ErrorCode(str, Enum):
    """业务错误码"""
    # 认证授权（1xxx）
    UNAUTHORIZED = "1001"
    FORBIDDEN = "1002"
    TOKEN_EXPIRED = "1003"
    
    # 文件上传（2xxx）
    FILE_TOO_LARGE = "2001"
    FILE_TYPE_NOT_ALLOWED = "2002"
    FILE_CORRUPTED = "2003"
    FILE_EMPTY = "2004"
    
    # 存储服务（3xxx）
    STORAGE_SERVICE_ERROR = "3001"
    STORAGE_QUOTA_EXCEEDED = "3002"
    STORAGE_CONNECTION_ERROR = "3003"
    
    # 数据库操作（4xxx）
    DATABASE_ERROR = "4001"
    DUPLICATE_ENTRY = "4002"
    RECORD_NOT_FOUND = "4003"
    
    # 任务队列（5xxx）
    TASK_QUEUE_ERROR = "5001"
    TASK_TIMEOUT = "5002"
    
    # 系统错误（9xxx）
    INTERNAL_ERROR = "9001"
    SERVICE_UNAVAILABLE = "9002"
    

class BaseAppException(Exception):
    """应用基础异常类"""
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class FileValidationError(BaseAppException):
    """文件验证异常类"""
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        details: Optional[Dict] = None
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )
        
        
class StorageServiceError(BaseAppException):
    """存储服务异常类"""
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        details: Optional[Dict] = None
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            details=details,
        )
        
        
class DatabaseError(BaseAppException):
    """数据库操作异常类"""
    def __init__(
        self,
        message: str, 
        error_code: ErrorCode,
        details: Optional[Dict] = None
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )
        

class TaskQueueError(BaseAppException):
    """任务队列异常类"""
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        details: Optional[Dict] = None
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )
