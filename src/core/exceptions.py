from typing import Optional, Dict, Any
from fastapi import HTTPException, status


# ======= 基础异常类定义 =======
class BaseAppException(Exception):
    """
    应用基础异常类
    所有自定义异常类均应继承自该类
    """
    def __init__(
        self,
        message: str,
        error_code: str = "internal_error",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)
        
    def __str__(self):
        return f"[{self.error_code}] {self.message}"
    
# ======= 通用业务异常 (按 HTTP 状态码分类) =======
# 400 Bad Request 类   
class ValidationError(BaseAppException):
    """参数校验失败"""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="validation_error",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )
          
class BusinessLogicError(BaseAppException):
    """业务逻辑错误"""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="business_error",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )

# 401/403 Auth 类
class AuthenticationError(BaseAppException):
    """
    认证授权错误，例如：未登录、令牌过期等
    """
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(
            message=message,
            error_code="authentication_failed",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
       
class PermissionDeniedError(BaseAppException):
    """权限不足错误"""
    def __init__(self, message: str = "Permission denied"):
        super().__init__(
            message=message,
            error_code="permission_denied",
            status_code=status.HTTP_403_FORBIDDEN,
        )

# 404 Not Found 类 
class NotFoundError(BaseAppException):
    """资源不存在"""
    def __init__(self, resource: str, resource_id: Any = None):
        msg = f"{resource} not found"
        if resource_id:
            msg += f": {resource_id}"
        super().__init__(
            message=msg,
            error_code="resource_not_found",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"resource": resource, "id": resource_id},
        )
        
# 409 Conflict 类
class ResourceConflictError(BaseAppException):
    """资源冲突（如重复创建）"""
    def __init__(self, message: str, error_code: str = "resource_conflict"):
        super().__init__(
            message=message,
            error_code=error_code,
            status_code=status.HTTP_409_CONFLICT,
        )
      
# ======= 基础设施/服务端异常 (5xx 类) =======  
class ExternalServiceError(BaseAppException):
    """外部服务调用失败 (例如: S3, LLM, VectorDB)"""
    def __init__(
        self, 
        service_name: str, 
        message: str = "Service temporarily unavailable",
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            error_code="external_service_unavailable",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            details=details or {}
        )
        self.details["service"] = service_name

class DatabaseError(BaseAppException):
    """数据库操作失败"""
    def __init__(self, message: str = "Database operation failed"):
        super().__init__(
            message=message,
            error_code="database_error",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
class VectorStoreError(BaseAppException):
    """向量存储服务错误"""
    def __init__(self, message: str = "Vector store operation failed"):
        super().__init__(
            message=message,
            error_code="vector_store_error",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

# ======= 特定领域异常 =======
class FileTooLargeError(BusinessLogicError):
    """文件过大异常类"""
    def __init__(self, limit_mb: int):
        super().__init__(
            message=f"File size exceeds limit of {limit_mb}MB",
            error_code="file_too_large",
            details={"limit_mb": limit_mb},
        )