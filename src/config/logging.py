import os
import logging
import importlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any
from src.config.settings import settings

SERVICE_NAME = settings.PROJECT_NAME
ENVIRONMENT = settings.ENVIRONMENT

class SimpleJsonFormatter(logging.Formatter):
    """自定义的 JSON 格式化器，用于结构化日志输出"""
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "levelname": record.levelname,
            "filename": record.filename,
            "lineno": record.lineno,
            "funcName": record.funcName,
            "request_id": getattr(record, "request_id", ""),
            "task_id": getattr(record, "task_id", ""),
            # "logger": record.name,
            "message": record.getMessage(),
        }

        # 合并额外字段（extra），排除内建字段
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in {
                "msg", "args", "levelname", "levelno", "name", "pathname", "filename",
                "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
                "created", "msecs", "relativeCreated", "thread", "threadName",
                "processName", "process",
            }:
                continue
            payload[k] = v

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


# 日志文件路径（可通过环境变量覆盖，默认使用相对可写路径以便跨平台）
FILE_LOG_PATH = os.getenv("APP_LOG_FILE", os.getenv("FILE_LOG_PATH", str(Path.cwd() / "logs" / "app.log")))

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
    
_handlers: Dict[str, Any] = {
    "console": {
        "class": "logging.StreamHandler",
        "formatter": "default" if ENVIRONMENT == "development" else "json",  # 开发环境使用默认格式，生产环境使用 JSON 格式
        "stream": "ext://sys.stdout",
    }
}

if _enable_file_handler:
    # 可选：仅当在非容器环境需要文件时启用
    _handlers["file"] = {
        "class": "logging.handlers.RotatingFileHandler",
        "formatter": "json",
        "filename": FILE_LOG_PATH,
        "maxBytes": 10 * 1024 * 1024,
        "backupCount": 5,
        "encoding": "utf-8",
    }

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            # "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            # "fmt": "%(asctime)s %(levelname)s %(name)s %(message)s",
            "()": "src.config.logging.SimpleJsonFormatter",
        },
        "default": {
           "()": "logging.Formatter",
            "format": "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s]%(request_id)s%(task_id)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": _handlers,
    "root": {
        "level": os.getenv("APP_LOG_LEVEL", "INFO"),
        "handlers": ["console"] + (["file"] if _enable_file_handler else []),
    },
    "loggers": {
        "uvicorn.access": {"level": "INFO", "handlers": ["console"], "propagate": False},
        "uvicorn.error": {"level": "INFO", "handlers": ["console"], "propagate": False},
        "myapp": {"level": "INFO", "handlers": ["console"], "propagate": False},
    },
}