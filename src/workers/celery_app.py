from celery import Celery
from redis import Redis
from redis.connection import ConnectionPool
import logging, logging.config
import platform
import multiprocessing

from src.config.settings import settings
from src.config.logging import logging_config
from src.middleware.request_id import setup_custom_log_record_factory
from src.workers import celery_config

setup_custom_log_record_factory()

logging_config["root"]["level"] = settings.APP_LOG_LEVEL
logging.config.dictConfig(logging_config)

IS_WINDOWS = platform.system().lower().startswith("windows")
DEFAULT_CONCURRENCY = settings.CELERY_WORKER_CONCURRENCY or max(1, multiprocessing.cpu_count() * 2)

# # 创建Redis连接池
# redis_pool = ConnectionPool(
#     host=settings.REDIS_HOST,
#     port=settings.REDIS_PORT,
#     db=settings.REDIS_DB,
#     socket_connect_timeout=5,
#     socket_timeout=5,
#     retry_on_timeout=True,
#     health_check_interval=30,  # 每30秒检查连接健康
# )


# 创建Celery实例
celery_app = Celery(
    "llm_service_worker",
    broker=settings.CELERY_BROKER_URL,  # 指定消息代理中间件
    backend=settings.CELERY_RESULT_BACKEND,  # 指定结果存储后端
)

celery_app.conf.update(
    # 设置任务序列化和反序列化方式
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    
    # 设置时区
    timezone="Asia/Shanghai",
    enable_utc=False,  # 不使用 UTC 时间
    
    # 结果存储
    result_expires=3600,  # 结果过期时间
    
    # Broker 连接
    broker_heartbeat=10,  # 心跳间隔（秒），防止空闲断开
    broker_connection_timeout=30,  # 连接超时时间（秒）
    broker_connection_retry=True,  # 连接失败时自动重试
    broker_connection_retry_on_startup=True,  # 启动时也自动重试
    broker_connection_max_retries=0,  # 无限重试
    broker_pool_limit=100,  # 连接池最大连接数
    
    # 添加 Redis 连接池配置
    broker_transport_options={
        # 'connection_pool': redis_pool
        'visibility_timeout': 3600,  # 任务在队列中的可见性超时，防止任务丢失
        'max_connections': 100,  # 最大连接数
        "socket_timeout": 30,  # 连接超时
        "socket_connect_timeout": 10,  # 连接建立超时
        'retry_on_timeout': True,  # 超时时自动重试
        'socket_keepalive': True,  # 启用 TCP keepalive
    },
    
    # 结果后端 Redis 连接池配置
    result_backend_transport_options={
        "socket_timeout": 30,  # 连接超时
        "socket_connect_timeout": 10,  # 连接建立超时
        'retry_on_timeout': True,  # 超时时自动重试
        'socket_keepalive': True,  # 启用 TCP keepalive
        'health_check_interval': 30,  # 每30秒进行一次健康检查
    },
    
    # 任务确认与丢失保护
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # 当 broker 断连时，是否取消正在运行的长任务（设置 False 可避免短暂断连取消执行中的任务）
    worker_cancel_long_running_tasks_on_connection_loss=False,
    
    # 日志配置
    # worker_log_format="[%(asctime)s]%(levelname)s in %(module)s: %(message)s",
    # worker_task_log_format="[%(asctime)s]%(levelname)s in %(task_name)s: %(message)s",
    # worker_log_level=logging.INFO,  # 生产环境通常使用 INFO 级别
    
    # 配置 Celery 日志使用自定义配置
    worker_hijack_root_logger = False  # 避免 Celery 修改根日志器配置
    # 在任务里显式使用 logging.getLogger(__name__)
    # 并从任务 args/headers 获取并设置 request_id 到上下文以便日志关联
)

if IS_WINDOWS:
    # Windows 平台特定配置（开发/测试用）
    celery_app.conf.update(
        worker_pool="solo",  # Windows 平台使用 solo，避免多进程/句柄/序列化问题
        worker_concurrency=1,  # 限制并发数为1，避免多进程问题
        worker_max_tasks_per_child=50,  # 减少子进程最大任务数，防止内存泄漏
        worker_prefetch_multiplier=1,  # 减少预取数量
        task_soft_time_limit=None,  # 禁用 soft timeout
    )
else:
    # 生产环境配置（推荐 Linux）
    celery_app.conf.update(
        worker_pool="eventlet",
        worker_concurrency=DEFAULT_CONCURRENCY,
        worker_max_tasks_per_child=100,
        worker_prefetch_multiplier=1,
        worker_pool_restarts=True,  # 定期重启 worker pool 防止内存泄漏
        task_soft_time_limit=300,  # 任务软超时，单位秒
        task_time_limit=3600,  # 任务硬超时，单位秒
        task_default_retry_delay=10,  # 任务重试间隔
        task_max_retries=5,  # 任务最大重试次数  
        task_acks_late=True,  # 确保任务被确认以防丢失
    )
    

# Celery Beat 定时任务配置
celery_app.conf.update(
    beat_schedule=celery_config.CELERY_BEAT_SCHEDULE,
    task_routes=celery_config.CELERY_TASK_ROUTES
)


# 自动发现任务（推荐）
# celery_app.autodiscover_tasks(["src.workers.user.email_notification"])
# celery_app.autodiscover_tasks(["src.workers.document.process_document"])
# celery_app.autodiscover_tasks(["src.workers.system.regular_tasks"])
celery_app.autodiscover_tasks(["src.workers"])  # 在主 workers 包的 __init__.py 中导入所有子模块


__all__ = ["celery_app"]