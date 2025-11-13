from celery.schedules import crontab
from datetime import timedelta


# Beat 调度器配置
CELERY_BEAT_SCHEDULE = {
    # 每天凌晨2点执行永久删除
    "permanent-delete-daily": {
        "task": "src.workers.document.process_document.schedule_permanent_deletion_task",
        "schedule": crontab(hour=2, minute=0),  # 每天凌晨2点执行
        "args": (),
        "options": {
            "expires": 3600,  # 任务过期时间，防止堆积
        }
    },
    
    # 可选：每6小时执行一次（用于测试或更频繁的清理）
    "permanent-delete-frequent": {
        "task": "src.workers.document.process_document.schedule_permanent_deletion_task",
        "schedule": timedelta(hours=6),  # 每6小时执行一次
        "args": (),
        "options": {
            "expires": 1800,  # 任务过期时间，防止堆积
        }
    },
    
    # 每天凌晨2点执行清理未确认用户
    "cleanup-unconfirmed-users": {
        "task": "src.workers.system.regular_tasks.cleanup_unconfirmed_users",
        "schedule": crontab(hour=2, minute=0),  # 每天凌晨2点执行
        # "schedule": timedelta(seconds=60), 
        "args": (),
        "options": {
            "queue": "deletion",  # 定义定时任务执行时使用的队列
            "expires": 3600,  # 任务过期时间，防止堆积
        }
    },
    
    "cleanup-deleted-users": {
        "task": "src.workers.system.regular_tasks.cleanup_deleted_users",
        "schedule": crontab(hour=2, minute=0),  # 每天凌晨2点执行
        # "schedule": timedelta(seconds=60), 
        "args": (),
        "options": {
            "queue": "deletion",  # 定义定时任务执行时使用的队列
            "expires": 3600,  # 任务过期时间，防止堆积
        }
    }
}

# 任务路由配置，根据任务类型分配到不同的队列
CELERY_TASK_ROUTES = {
    "src.workers.document.process_document._find_expired_documents": {
        "queue": "query"
    },
    "src.workers.document.process_document.permanent_delete_document_task": {
        "queue": "deletion",
        "rate_limit": "10/m"  # 限制删除速率，任务每分钟最多执行10次
    },
    "src.workers.document.process_document.schedule_permanent_deletion_task": {
        "queue": "scheduler"
    },
    # 可以使用通配符来匹配多个任务
    "src.workers.system.regular_tasks.*": {
        "queue": "deletion",
        "rate_limit": "10/m" 
    },
}

