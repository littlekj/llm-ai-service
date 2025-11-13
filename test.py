from src.workers.document.process_document import delete_document_from_s3_task

from src.utils.minio_storage import MinioClient
from src.config.settings import settings

import logging

logger = logging.getLogger(__name__)


# from datetime import datetime, timezone
# current_time = datetime.now(timezone.utc)
# def parse_iso_datetime(s: str) -> datetime:
#     """
#     解析 ISO 8601 格式的日期时间字符串为 datetime 对象
#     """
#     try:
#         return datetime.fromisoformat(s.replace('Z', '+00:00'))
#     except:
#         return datetime.min
# print("Current UTC time: ", current_time)
# print("ISO format: ", current_time.isoformat())
# print("Parsed time: ", parse_iso_datetime(current_time.isoformat()))

# logger.info("Test script completed.")
# logger.debug("This is a debug message.")
# logger.warning("This is a warning message.")
# logger.error("This is an error message.")
# logger.critical("This is a critical message.")


# from src.workers.system.regular_tasks import cleanup_unconfirmed_users
# cleanup_unconfirmed_users.delay()
