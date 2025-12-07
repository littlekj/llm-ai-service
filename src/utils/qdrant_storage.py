import logging
import asyncio
import httpx
from typing import List, Dict, Optional, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient as BaseQdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, MatchAny
from qdrant_client.http.models import SearchRequest, VectorParams, Distance
from qdrant_client.http.models import PointStruct
from qdrant_client.http.exceptions import UnexpectedResponse, ResponseHandlingException

from src.config.settings import settings
from src.core.exceptions import VectorStoreError


logger = logging.getLogger(__name__)

class QdrantClient:
    def __init__(
        self,
        collection_name: str = "documents",
        embeddings_model: Optional[str] = None,
        vector_size: int = 384,  # 默认向量维度，需与 Embeddings 模型输出一致
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
        self.vector_size = vector_size
        
        # 加载 Embeddings 模型
        try:
            self.model = SentenceTransformer(embeddings_model)
            logger.info(f"Loaded embeddings model: {embeddings_model}")
        except Exception as e:
            logger.error(f"Failed to load embeddings model: {e}", exc_info=True)
            raise
        
        # 确保集合存在
        self._ensure_collection_exists()
        
    def _ensure_collection_exists(self):
        """确保向量集合存在，否则创建"""
        try:
            self.client.get_collection(self.collection_name)
            # logger.info(f"Collection '{self.collection_name}' already exists")
        except Exception:
            try:
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE,
                    ),
                )
                logger.info(f"Created collection '{self.collection_name}'")
            except Exception as e:
                logger.error(f"Failed to create collection: {e}", exc_info=True)
                raise
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=8),
    )
    async def add_chunks(
        self,
        chunks: List[Dict[str, Any]],
        document_id: UUID,
        user_id: UUID,
    ) -> Dict[str, Any]:
        """
        将文档片段向量化并添加到向量存储
        
        :param chunks: 文档片段列表，每个片段包含 'content' 和 'metadata'
        :param document_id: 文档 ID
        :param user_id: 用户 ID
        :return: 添加结果统计
        """
        logger.info(f"Starting to add {len(chunks)} chunks to vector store")
        try:
            # 批量生成向量
            loop = asyncio.get_event_loop()
            texts = [chunk["content"] for chunk in chunks]
            embeddings = await loop.run_in_executor(
                None,
                self._batch_embed,
                texts,
            )
            
            # 准备点数据
            points = []
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                point_id = str(uuid4())  # 为每个块生成唯一 ID
                
                metadata = chunk.get("metadata", {})
                full_metadata = {
                    "document_id": str(document_id),
                    "user_id": str(user_id),
                    "chunk_index": metadata.get("chunk_index", i),
                    "page_number": metadata.get("page_number"),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                
                point = PointStruct(
                    id=point_id,
                    vector=embedding.tolist() if hasattr(embedding, "tolist") else embedding,
                    payload={
                        "content": chunk["content"],
                        "metadata": full_metadata,
                    }
                )
                points.append(point)
            
            # 批量上传到 Qdrant
            await loop.run_in_executor(
                None,
                self.client.upsert,
                self.collection_name,
                points,
            )
            
            logger.info(f"Successfully added {len(chunks)} chunks to vector store")
            
            return {
                "added_count": len(points),
                "document_id": str(document_id),
                "user_id": str(user_id),
                "points": points,  # 返回点数据以供后续使用
            }
            
        except Exception as e:
            logger.error(f"Failed to add chunks to vector store: {e}", exc_info=True)
            raise
    
    def _batch_embed(self, texts: List[str]) -> List[List[float]]:
        """
        批量生成向量（同步方法，在线程池中执行）生成文本向量
        """
        try:
            embeddings = self.model.encode(texts, convert_to_tensor=False)
            return embeddings.tolist()
        except Exception as e:
            logger.error(f"Batch embedding error: {str(e)}", exc_info=True)
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
        min_score: float = 0.3,
    ) -> List[Dict[str, Any]]:
        """基于文本相似度搜索文档片段"""
        
        # 调用 Embeddings 模型将查询文本转换为向量
        query_vector = await self._get_embeddings(query)
        if not query_vector:
            logging.warning("Embeddings generation returned empty vector")
            raise ValueError("Failed to generate embeddings for query")
        
        try:
            # 构建搜索过滤条件
            search_filter = None
            if filter_doc_ids and len(filter_doc_ids) > 0:
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
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                self._search_sync,
                query_vector,
                search_filter,
                limit,
            )
            # results = self.client.search(
            #     collection_name=self.collection_name,
            #     query_vector=query_vector,
            #     query_filter=search_filter,
            #     limit=limit,
            # )
            
            # 格式化返回结果
            chunks = []
            for result in results:
                try:
                    if result.score < min_score:
                        continue
                    payload = result.payload or {}
                    metadata = payload.get("metadata", {})
                    
                    chunks.append({
                        "content": payload.get("content", ""),
                        "metadata": metadata,
                        "score": result.score,
                        "point_id": result.id,
                    })
                except Exception as e:
                    # 处理单个结果解析失败，跳过并记录错误
                    logger.error(f"Skipping malformed vector search result: {e}", exc_info=True)
                    continue
                
            logger.info(f"Similarity search found {len(chunks)} chunks")
            return chunks

        except UnexpectedResponse as e:
            # 服务端响应异常
            logger.error(f"Qdrant server error during vector search: {str(e)}", exc_info=True)
            raise VectorStoreError(message="Qdrant server error during vector search") from e
        except ResponseHandlingException as e:
            # 响应解析异常
            logger.error(f"Failed to parse Qdrant response: {str(e)}", exc_info=True)
            raise VectorStoreError(message="Failed to parse Qdrant response") from e
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
            # 网络连接/超时异常
            logger.error(f"Network error connecting to Qdrant: {str(e)}", exc_info=True)
            raise VectorStoreError(message="Network error connecting to Qdrant") from e
        except Exception as e:
            # 捕获所有其他异常
            logger.error(f"Unexpected vector search error: {str(e)}", exc_info=True)
            raise VectorStoreError(message="Unexpected vector search error") from e
        
    def _search_sync(self, query_vector, search_filter, limit):
        """同步搜索方法"""
        return self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=limit,
        )
        
    async def _get_embeddings(self, text: str) -> List[float]:
        """
        获取文本的向量表示
        这里可以调用不同的 Embeddings 模型：
        - OpenAI Embeddings
        - HuggingFace Embeddings
        - 自定义模型
        """
        # TODO: 实现具体的 embeddings 逻辑
        
        # 使用 sentence-transformers 将文本转换为向量
        embeddings = self.model.encode(text)
        return embeddings.tolist()
        
    async def delete_chunks_by_doc_id(self, doc_id: UUID):
        """根据文档 ID 删除所有相关的向量"""
        try:
            logger.info(f"Deleting chunks for document ID: {doc_id}")
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.client.delete,
                self.collection_name,
                Filter(
                    must=[
                        FieldCondition(
                            key="metadata.document_id",
                            match=MatchValue(value=str(doc_id))
                        )
                    ]
                ),
            )
            
            logger.info(f"Successfully deleted chunks for document ID: {doc_id}")
        
        except Exception as e:
            logger.error(f"Error deleting chunks for document: {str(e)}", exc_info=True)
            raise
       