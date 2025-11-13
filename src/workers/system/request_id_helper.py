from typing import Optional
import logging

from src.middleware.request_id import request_id_ctx_var

logger = logging.getLogger(__name__)


def extract_request_id_from_celery_request(celery_request) -> Optional[str]:
    """
    从 Celery 任务请求对象中提取 request_id（优先 headers -> kwargs -> message）
    不抛出异常，失败时返回 None
    """
    if not celery_request:
        return None
    
    try:
        # 尝试从 headers 中提取 request_id
        hdrs = getattr(celery_request, "headers", None)
        if isinstance(hdrs, dict):
            rid = hdrs.get("request_id") or hdrs.get("x-request-id")
            if rid:
                return rid
        
        # 尝试从 kwargs 中提取 request_id
        kw = getattr(celery_request, "kwargs", None)
        if isinstance(kw, dict):
            rid = kw.get("request_id") or kw.get("x-request-id")
            if rid:
                return rid
        
        # 尝试从 message 对象中提取 request_id 
        message = getattr(celery_request, "message", None)
        if message is not None:
            try:
                msg_hdrs = getattr(message, "headers", None) or getattr(message, "properties", None)
                if isinstance(msg_hdrs, dict):
                    rid = msg_hdrs.get("request_id") or msg_hdrs.get("x-request-id")
                    if rid:
                        return rid
            except Exception:
                # 忽略 message 解析错误
                pass
    except Exception:
        logger.debug("Failed to extract request_id from celery request", exc_info=True)
        
    return None

def set_request_id_from_task(task_self) -> Optional[str]:
    """
    从 Celery 任务实例中提取 request_id 并设置到上下文变量
    适用于在任务执行开始时调用，以确保日志中包含 request_id
    """
    try:
        celery_req = getattr(task_self, "request", None)
        rid = extract_request_id_from_celery_request(celery_req)
        # 备选方案：尝试从任务参数 kwargs 中提取 request_id
        if not rid:
            try:
                maybe_kwargs = getattr(celery_req, "kwargs", None)
                if isinstance(maybe_kwargs, dict):
                    rid = maybe_kwargs.get("request_id") or maybe_kwargs.get("x-request-id")
            except Exception:
                pass
            
        if rid:
            request_id_ctx_var.set(rid)
            return rid
    except Exception:
        # 静默失败，不影响任务执行
        logger.debug("set_request_id_from_task_failed", exc_info=True)
        
    return None

