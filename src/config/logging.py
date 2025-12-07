import os
import logging
import importlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any
from src.config.settings import settings
from src.middleware.request_id import request_id_ctx_var

SERVICE_NAME = settings.PROJECT_NAME
ENVIRONMENT = settings.ENVIRONMENT


class ContextFilter(logging.Filter):
    """
    自定义的日志过滤器，用于在日志中添加 request_id 和 task_id
    - 自动从 ContextVar 获取 request_id 和 task_id 并注入到 LogRecord。
    - 确保 request_id 和 task_id 字段始终存在，防止 TextFormatter 报错。
    """
    def filter(self, record: logging.LogRecord) -> bool:
        # 获取 request_id 和 task_id
        # 优先使用 extra 传入的，其次使用 ContextVar，最后默认为空
        if not getattr(record, "request_id", None):
            ctx_id = request_id_ctx_var.get()
            record.request_id = ctx_id if ctx_id else ""
            
        # 获取 task_id（Celery 场景）
        if not getattr(record, "task_id", None):
            try:
                from celery import current_task
                if current_task:
                    tid = getattr(current_task.request, "id", None)
                    record.task_id = tid if tid else ""
                else:
                    record.task_id = ""
            except Exception:
                record.task_id = ""
                
        return True
    
# 定义 LogRecordFactory (替代 ContextFilter)
def setup_log_record_factory():
    """
    安装自定义 LogRecordFactory。
    - 在 LogRecord 对象创建时，自动注入 request_id 和 task_id。
    - 对所有 Logger（包括第三方库）生效，无需在 Handler 中配置 Filter。
    """
    # 获取原有的 factory，保留原有逻辑
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        # 创建标准的 LogRecord
        record = old_factory(*args, **kwargs)

        # --- 注入 request_id ---
        # 检查是否已有 request_id（通过 extra 传入）
        if not hasattr(record, "request_id"):
            # 尝试从 ContextVar 获取，默认为空字符串
            req_id = request_id_ctx_var.get()
            record.request_id = req_id if req_id else ""

        # --- 注入 task_id (Celery) ---
        # 检查是否已存在 task_id（通过 extra 传入）
        if not hasattr(record, "task_id"): 
            # 默认为空
            record.task_id = ""
            try:
                # 懒加载 celery，防止循环导入或未安装报错
                from celery import current_task
                if current_task:
                    # 获取 Celery Task ID
                    tid = getattr(current_task.request, "id", None)
                    record.task_id = tid if tid else ""
                    
            except (ImportError, AttributeError, Exception):
                pass
            
        return record
    
    # 设置为全局 factory
    logging.setLogRecordFactory(record_factory)

class SimpleJsonFormatter(logging.Formatter):
    """
    自定义的 JSON 格式化器，用于结构化日志输出
    - 
    """
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "levelname": record.levelname,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "message": record.getMessage(),
            "request_id": record.request_id,
            "task_id": record.task_id,
        }
        
        # 异常堆栈处理
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # 合并额外字段（extra），排除内建字段
        # 这使得 logger.info("msg", extra={"user_id": 1}) 中的 user_id 变成 JSON 顶层字段
        skip_keys = {
            "msg", "args", "levelname", "levelno", "name", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "request_id", "task_id" # 排除已处理的字段
        }
        
        for k, v in record.__dict__.items():
            if k not in skip_keys and not k.startswith("_"):
                payload[k] = v

        return json.dumps(payload, ensure_ascii=False)


# 日志文件路径（可通过环境变量覆盖，默认使用相对可写路径以便跨平台）
FILE_LOG_PATH = os.getenv("APP_LOG_FILE", os.getenv("FILE_LOG_PATH", str(Path.cwd() / "logs" / "app.log")))
# 检查日志文件路径是否可写
_enable_file_handler = False
try:
    file_parent = Path(FILE_LOG_PATH).parent
    file_parent.mkdir(parents=True, exist_ok=True)
    test_path = file_parent / (".write_test")
    try:
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok")
        test_path.unlink(missing_ok=True)
        _enable_file_handler = True
    except Exception:
        _enable_file_handler = False
except Exception:
    _enable_file_handler = False

# 格式化器选择
formatter_name = "json" if ENVIRONMENT == "production" else "default"

# 日志配置
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    
    # "filters": {
    #     "context_filter": {
    #         "()": ContextFilter,  # 挂载自定义过滤器
    #     }
    # },
    
    "formatters": {
        "json": {
            # "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            # "fmt": "%(asctime)s %(levelname)s %(name)s %(message)s",
            "()": "src.config.logging.SimpleJsonFormatter",
        },
        "default": {
           "()": "logging.Formatter",
            "format": "[%(asctime)s.%(msecs)03d] [%(levelname)s] [ReqID:%(request_id)s] [%(module)s] [%(funcName)s:%(lineno)d]: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            # "filters": ["context_filter"],  # 挂载过滤器
            "formatter": formatter_name,
            "stream": "ext://sys.stdout",
        },
    },
    
    "root": {
        "level": os.getenv("APP_LOG_LEVEL", "INFO"),
        "handlers": ["console"] + (["file"] if _enable_file_handler else []),
    },
    
    "loggers": {
        # 根日志记录器
        "root": {
            "level": os.getenv("APP_LOG_LEVEL", "INFO"),
            "handlers": ["console"] + (["file"] if _enable_file_handler else []),
        },
        # 业务代码日志
        "src": {
            "level": "INFO",
            "propagate": True,
        },
        # Uvicorn 访问日志 (接管格式)
        "uvicorn.access": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False, 
        },
        "uvicorn.error": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
        # 第三方库降噪 (防止 boto3/sqlalchemy 刷屏)
        "boto3": {"level": "WARNING"},
        "botocore": {"level": "WARNING"},
        "urllib3": {"level": "WARNING"},
        "sqlalchemy.engine": {"level": "WARNING"}, # INFO 级别可查看 SQL 语句
    },
}

if _enable_file_handler:
    # 可选：仅当在非容器环境需要文件时启用
    logging_config["handlers"]["file"] = {
        "class": "logging.handlers.RotatingFileHandler",
        # "filters": ["context_filter"],  # 挂载过滤器
        "formatter": "json",  # 文件日志使用 JSON 格式
        "filename": FILE_LOG_PATH,
        "maxBytes": 10 * 1024 * 1024,
        "backupCount": 5,
        "encoding": "utf-8",
    }