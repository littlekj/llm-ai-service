from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from contextvars import ContextVar
from typing import Optional
import uuid
import logging


# 用于存储请求 ID 的上下文变量
request_id_ctx_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)

class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        """为每个请求生成唯一的 Request ID，并将其添加到响应头中
        请求 ID 中间件：
        - 生成或获取 X-Request-ID 
        - 设置到
        """
        # 优先使用请求头中的 ID，否则生成新的
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        
        # 设置到 ContextVar，LogRecordFactory 后续读取
        token = request_id_ctx_var.set(request_id)
        
        try:
            response = await call_next(request)
            # 将 Request ID 添加到响应头中
            response.headers["x-request-id"] = request_id
            return response
        finally:
            # 清除上下文变量
            request_id_ctx_var.reset(token)
