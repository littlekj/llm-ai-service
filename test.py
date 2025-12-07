from src.workers.document.object_storage import delete_document_from_s3_task

from src.utils.minio_storage import MinioClient
from src.config.settings import settings

import logging

logger = logging.getLogger(__name__)


# # 在 Python 控制台或脚本中运行
# from src.utils.qdrant_storage import QdrantClient

# client = QdrantClient()

# # 获取集合信息
# collection_info = client.client.get_collection(collection_name="documents")
# print(f"Collection info: {collection_info}")
# print(f"Total points: {collection_info.points_count}")

# # 滚动浏览所有点（查看前 10 个）
# scroll_result = client.client.scroll(
#     collection_name="documents",
#     limit=10,
#     with_payload=True,
#     with_vectors=False,
# )

# print(f"\n=== First 10 points ===")
# for point in scroll_result[0]:
#     print(f"Point ID: {point.id}")
#     print(f"Payload: {point.payload}")
#     print("---")


import socket
import time
import redis
from src.config.settings import settings

def test_redis_keepalive():
    """测试 Redis keepalive 配置"""
    client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        socket_keepalive=True,
        socket_keepalive_options={
            socket.TCP_KEEPIDLE: 60,
            socket.TCP_KEEPINTVL: 10,
            socket.TCP_KEEPCNT: 3,
        },
    )
    
    print("Setting test key...")
    client.set('test_keepalive', 'value')
    
    print("Waiting 5 minutes to test keepalive...")
    for i in range(30):  # 5 分钟
        time.sleep(10)
        try:
            client.get('test_keepalive')
            print(f"✅ [{i*10}s] Connection still alive")
        except Exception as e:
            print(f"❌ [{i*10}s] Connection failed: {e}")
            break
    
    print("Test completed!")

if __name__ == "__main__":
    test_redis_keepalive()