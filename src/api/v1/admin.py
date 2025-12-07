import logging
import time
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.depends import get_async_session, get_current_user
from src.models.user import User, UserRole
from src.config.settings import settings

# optional imports
try:
    from qdrant_client import QdrantClient as BaseQdrantClient
except Exception:
    BaseQdrantClient = None

try:
    import redis
except Exception:
    redis = None

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/usage")
async def admin_usage(
    db: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
):
    """管理员查看用户配额使用情况（需 admin 权限）

    返回示例：
    ```
    {
      "total_users": 100,
      "active_users": 90,
      "total_quota": 1000000,
      "total_used": 123456,
      "top_users": [ {"id": "..", "username":"..", "used_tokens": 1234, "quota_tokens": 10000} ]
    }
    ```
    """
    if not current_user or getattr(current_user, "role", None) != UserRole.ADMIN.value:
        raise HTTPException(status_code=403, detail="Admin privileges required")

    try:
        # 聚合统计
        total_users_stmt = select(func.count()).select_from(User)
        active_users_stmt = select(func.count()).select_from(User).where(User.is_active == True)
        total_quota_stmt = select(func.coalesce(func.sum(User.quota_tokens), 0))
        total_used_stmt = select(func.coalesce(func.sum(User.used_tokens), 0))

        total_users_res = await db.execute(total_users_stmt)
        active_users_res = await db.execute(active_users_stmt)
        total_quota_res = await db.execute(total_quota_stmt)
        total_used_res = await db.execute(total_used_stmt)

        total_users = int(total_users_res.scalar() or 0)
        active_users = int(active_users_res.scalar() or 0)
        total_quota = int(total_quota_res.scalar() or 0)
        total_used = int(total_used_res.scalar() or 0)

        # top users by usage
        top_stmt = select(User.id, User.username, User.used_tokens, User.quota_tokens).order_by(User.used_tokens.desc()).limit(10)
        top_res = await db.execute(top_stmt)
        top_users = [
            {"id": str(row.id), "username": row.username, "used_tokens": int(row.used_tokens), "quota_tokens": int(row.quota_tokens)} 
            for row in top_res.all()
        ] 
        
        return {
            "total_users": total_users,
            "active_users": active_users,
            "total_quota": total_quota,
            "total_used": total_used,
            "top_users": top_users,
        }
        
    except Exception as e:
        logger.error(f"Failed to compute usage: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute usage")


@router.get("/metrics")
async def prometheus_metrics(response: Response, db: AsyncSession = Depends(get_async_session)):
    """Prometheus 指标端点

    提供两种格式的监控指标：
    1. 优先使用 prometheus_client 库生成标准 Prometheus 格式
    2. 如果 prometheus_client 不可用，回退到纯文本格式
    """
    # 尝试使用 prometheus_client 生成标准格式指标
    try:
        import prometheus_client
        from prometheus_client import CollectorRegistry, Gauge, generate_latest

        # 创建指标注册表
        registry = CollectorRegistry()

        # 从数据库获取基础统计数据
        total_users = int((await db.execute(select(func.count()).select_from(User))).scalar() or 0)
        active_users = int((await db.execute(select(func.count()).select_from(User).where(User.is_active == True))).scalar() or 0)
        active_rate = float(round(active_users / total_users * 100, 2)) if total_users > 0 else 0.0

        # 创建 Gauge 类型的指标
        g_total = Gauge("llm_total_users", "Total users", registry=registry)
        g_active = Gauge("llm_active_users", "Active users", registry=registry)
        g_active_rate = Gauge("llm_active_user_rate", "Active user rate percentage", registry=registry)

        # 设置指标值
        g_total.set(total_users)
        g_active.set(active_users)
        g_active_rate.set(active_rate)
        
        # 生成 Prometheus 格式的输出
        output = generate_latest(registry)
        response.headers["Content-Type"] = prometheus_client.CONTENT_TYPE_LATEST
        return Response(content=output, media_type=prometheus_client.CONTENT_TYPE_LATEST)

    except Exception:
        # 回退方案：生成纯文本格式的指标
        try:
            # 重新获取统计数据
            total_users = int((await db.execute(select(func.count()).select_from(User))).scalar() or 0)
            active_users = int((await db.execute(select(func.count()).select_from(User).where(User.is_active == True))).scalar() or 0)
            active_rate = float(active_users / total_users * 100) if total_users > 0 else 0.0

            # 构建符合 Prometheus 文本格式的指标
            lines = [
                f"# HELP llm_total_users Total number of users",
                f"# TYPE llm_total_users gauge",
                f"llm_total_users {total_users}",
                f"# HELP llm_active_users Active users",
                f"# TYPE llm_active_users gauge",
                f"llm_active_users {active_users}",
                f"# HELP llm_active_user_rate Active user rate percentage",
                f"# TYPE llm_active_user_rate gauge",
                f"llm_active_user_rate {active_rate}",
            ]
            content = "\n".join(lines) + "\n"
            response.headers["Content-Type"] = "text/plain; version=0.0.4"
            return Response(content=content, media_type="text/plain; version=0.0.4")
        except Exception as e:
            # 错误处理：记录日志并返回 HTTP 500
            logger.error(f"Failed to produce metrics: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to produce metrics")


@router.get("/health")
async def health_check(db: AsyncSession = Depends(get_async_session)):
    """健康检查端点：检查数据库、Redis 和 Qdrant 的连通性
    
    返回格式：
    ```json
    {
        "status": "ok",  # 整体状态 "ok" 或 "fail"
        "checks": {      # 各组件状态
            "database": {"status": "ok"},
            "redis": {"status": "ok"},
            "qdrant": {"status": "ok"},
            "minio": {"status": "ok"},
        },
        "elapsed_ms": 10  # 检查耗时（毫秒）
    }
    ```
    """
    report = {"status": "ok", "checks": {}}
    start = time.time()

    # 数据库连接检查
    try:
        # 执行简单查询测试数据库连通性
        await db.execute(select(func.now()))
        report["checks"]["database"] = {"status": "ok"}
    except Exception as e:
        logger.error(f"Database health check failed: {e}", exc_info=True)
        report["checks"]["database"] = {"status": "fail", "error": str(e)}

    # Redis 连接检查
    try:
        if redis is not None:
            # 优先使用显示配置的Redis主机设置
            if settings.REDIS_HOST:
                r = redis.Redis(
                    host=settings.REDIS_HOST, 
                    port=settings.REDIS_PORT or 6379, 
                    db=settings.REDIS_DB or 0,
                    password=settings.REDIS_PASSWORD.get_secret_value()
                )
            else:
                # 回退到使用Celery的Broker URL
                broker_url = settings.CELERY_BROKER_URL
                r = redis.from_url(broker_url)
            pong = r.ping()
            report["checks"]["redis"] = {"status": "ok" if pong else "fail"}
        else:
            report["checks"]["redis"] = {"status": "unknown", "note": "redis library not installed"}
    except Exception as e:
        logger.error(f"Redis health check failed: {e}", exc_info=True)
        report["checks"]["redis"] = {"status": "fail", "error": str(e)}

    # Qdrant 向量数据库检查
    try:
        if BaseQdrantClient is None:
            report["checks"]["qdrant"] = {"status": "unknown", "note": "qdrant-client not installed"}
        else:
            # 构建客户端参数
            client_args = {
                "url": settings.QDRANT_SERVER_URL,
                "api_key": settings.QDRANT_API_KEY.get_secret_value(),
                "https": settings.QDRANT_SERVER_URL.startswith("https"),  # 根据URL动态设置
                "verify": settings.QDRANT_SERVER_URL.startswith("https"),  # HTTPS时启用证书验证
            }
            client = BaseQdrantClient(**client_args)
            # 使用轻量级的get_collections接口检查连通性
            _ = client.get_collections()
            report["checks"]["qdrant"] = {"status": "ok"}
    except Exception as e:
        logger.error(f"Qdrant health check failed: {e}", exc_info=True)
        report["checks"]["qdrant"] = {"status": "fail", "error": str(e)}
        
    try:
        from src.utils.minio_storage import MinioClient
        minio_client = MinioClient(
            endpoint_url=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=settings.MINIO_SECURE,
            bucket_name=settings.MINIO_BUCKET_NAME
        )
        # 使用轻量级的bucket_exists接口检查连通性
        if minio_client.client.bucket_exists(settings.MINIO_BUCKET_NAME):
            report["checks"]["minio"] = {"status": "ok"}
        else:
            report["checks"]["minio"] = {"status": "fail", "error": "Bucket not accessible"}
        
    except Exception as e:
        logger.error(f"Minio health check failed: {e}", exc_info=True)
        report["checks"]["minio"] = {"status": "fail", "error": str(e)}

    # 计算检查耗时
    elapsed = time.time() - start
    report["elapsed_ms"] = int(elapsed * 1000)

    # 如果任何组件检查失败，将整体状态设为fail
    if any(c.get("status") == "fail" for c in report["checks"].values() if isinstance(c, dict)):
        report["status"] = "fail"

    return report
