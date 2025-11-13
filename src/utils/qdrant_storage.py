import logging
import os
from typing import List, Dict, Optional, Any
from uuid import UUID, uuid4
from tenacity import retry, stop_after_attempt, wait_exponential
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient as BaseQdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, MatchAny
from qdrant_client.http.models import SearchRequest

from src.config.settings import settings


logger = logging.getLogger(__name__)

class QdrantClient:
    def __init__(
        self,
        collection_name: str = "documents",
        embeddings_model: Optional[str] = None,
    ):
        """
        初始化 Qdrant 客户端
        """
        embeddings_model = embeddings_model or settings.EMBEDDING_MODEL_NAME
        # self.client = BaseQdrantClient(
        #     url=settings.QDRANT_SERVER_URL, 
        #     api_key=settings.QDRANT_API_KEY.get_secret_value(),
        #     https=False,  # 禁用 HTTPS，如果服务端使用 HTTP
        #     prefer_grpc=False,  # 禁用 gRPC，仅使用 HTTP
        #     timeout=10,  # 设置超时时间（秒）
        # )
        
        client_kwargs = {
            "url": settings.QDRANT_SERVER_URL,
            "api_key": settings.QDRANT_API_KEY.get_secret_value(),
            "https": settings.QDRANT_SERVER_URL.startswith("https"),  # 根据URL动态设置
            "verify": settings.QDRANT_SERVER_URL.startswith("https"),  # HTTPS时启用证书验证
            "prefer_grpc": False,  # 禁用 gRPC，仅使用 HTTP
            "timeout": 10,  # 设置超时时间（秒）
            "trust_env": False,  # 禁用环境变量中的代理设置
        }

        self.client = BaseQdrantClient(**client_kwargs)
        self.collection_name = collection_name
        
        # 加载 Embeddings 模型
        try:
            self.model = SentenceTransformer(embeddings_model)
            logger.info(f"Loaded embeddings model: {embeddings_model}")
        except Exception as e:
            logger.error(f"Failed to load embeddings model: {e}", exc_info=True)
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=8),
    )
    async def similarity_search(
        self,
        query: str,
        filter_doc_ids: Optional[List[UUID]] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """基于文本相似度搜索文档片段"""
        try:
            # 调用 Embeddings 模型将查询文本转换为向量
            query_vector = await self._get_embeddings(query)
            if not query_vector:
                raise ValueError("Failed to generate embeddings for query")
        
            # 构建搜索过滤条件
            search_filter = None
            if filter_doc_ids:
                search_filter = Filter(
                    must=[
                        FieldCondition(
                            key="metadata.document_id",
                            match=MatchAny(
                                any=[str(doc_id) for doc_id in filter_doc_ids]
                            )
                        )
                    ]
                )

            # 执行向量相似搜索
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                query_filter=search_filter,
                limit=limit,
            )
            
            # 格式化返回结果
            chunks = []
            
            for result in results:
                try:
                    payload = result.payload or {}
                    chunks.append(
                        {
                            "content": payload.get("content", ""),
                            "metadata": {
                                "document_id": UUID(payload["metadata"]["document_id"]),
                                "page_number": payload["metadata"].get("page_number"),
                                "score": result.score,
                            }
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to process vector search results: {e}", exc_info=True)
                    continue
                
            return chunks

        except Exception as e:
            logger.error(f"Vector search error: {str(e)}", exc_info=True)
            raise
        
    async def _get_embeddings(self, text: str) -> List[float]:
        """
        获取文本的向量表示
        这里可以调用不同的 Embeddings 模型：
        - OpenAI Embeddings
        - HuggingFace Embeddings
        - 自定义模型
        """
        # TODO: 实现具体的 embeddings 逻辑
        try:
            # 使用 sentence-transformers 将文本转换为向量
            embeddings = self.model.encode(text)
            return embeddings.tolist()
        
        except Exception as e:
            logger.error(f"Embedding generation error: {str(e)}", exc_info=True)
            raise
        
    async def add_texts(
        self,
        texts: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[UUID]] = None,
    ) -> List[UUID]:
        """
        将文本片段添加到向量存储
        :param texts: 文本片段
        :param metadatas: 文本元数据
        :param ids: 文本 ID
        """
        try:
            # 生成向量
            embeddings = [
                await self._get_embeddings(text) for text in texts
            ]
            
            # 准备点数据
            points = []
            for i, (text, embedding) in enumerate(zip(texts, embeddings)):
                point = {
                    "id": ids[i] if ids else uuid4(),
                    "vector": embedding,
                    "payload": {
                        "content": text,
                        "metadata": metadatas[i] if metadatas else {},
                    }
                }
                points.append(point)
            
            # 批量添加
            self.client.upsert(
                collection_name=self.collection_name,
                points=points,
            )
            
            return [point["id"] for point in points]
        
        except Exception as e:
            logger.error(f"Error adding texts to vector store: {str(e)}", exc_info=True)
            raise
                
def get_vector_store() -> QdrantClient:
    return QdrantClient()        