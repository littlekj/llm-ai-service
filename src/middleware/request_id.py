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
        """为每个请求生成唯一的 Request ID，并将其添加到响应头中"""
        rid = request.headers.get("x-request-id") or str(uuid.uuid4())
        request_id_ctx_var.set(rid)
        response = await call_next(request)
        response.headers["x-request-id"] = rid
        return response
        
def setup_custom_log_record_factory():
    """安装自定义的 LogRecord 工厂函数"""
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        try:
            rid = request_id_ctx_var.get()
            record.request_id = f" [request_id={rid}]" if rid else ""
        except Exception:
            record.request_id = ""
            
        try:
            from celery import current_task
            if current_task:
                tid = getattr(current_task.request, "id", None)
                record.task_id = f" [task_id={tid}]" if tid else ""
            else:
                record.task_id = ""
        except Exception:
            record.task_id = ""
            
        return record
    
    logging.setLogRecordFactory(record_factory)

        