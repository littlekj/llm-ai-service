import logging
import asyncio
import httpx
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config.settings import settings
from src.middleware.request_id import request_id_ctx_var
from src.schemas.session import ChatResponse

logger = logging.getLogger(__name__)

class LLMClient:
    """
    异步的LLM客户端，基于智谱/第三方兼容 OpenAI 的 API 接口实现
    """
    def __init__(
        self,
        model: Optional[str] = None,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,      
    ):
        self.model = model or settings.LLM_MODEL_NAME
        self.api_url = api_url or settings.LLM_API_URL
        self.api_key = api_key or settings.LLM_API_KEY.get_secret_value()
        if not self.api_key or not self.model:
            raise ValueError("LLM api_url and model must be configured in settings")
        
        # 禁用 SSL 验证并禁止从环境变量读取证书配置（production 请配置正确证书并移除这些选项）
        self._client = httpx.AsyncClient(timeout=timeout, verify=False, trust_env=False)
        
        logger.info("LLMClient initialized with model")

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass
        
    @retry(
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
    )
    async def chat_completion(
        self,
        messages: List[Dict[str, Any]],
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> ChatResponse:
        """
        调用智谱类 Chat Completion 接口，返回统一的 ChatResponse。
        messages: [{"role":"system|user|assistant", "content":"..."}]
        """
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "LLM-Service/1.0 (Python/httpx)"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        # 添加请求ID到请求头
        rid = request_id_ctx_var.get()
        if rid:
            headers["X-Request-ID"] = rid
        
        # 构造请求体
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": float(temperature),
        }
        if max_tokens:
            payload["max_tokens"] = int(max_tokens)
            
        try:
            resp = await self._client.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            
            # 尝试解析常见响应格式（兼容 OpenAI-like 和厂商返回）
            # 优先读取 choices -> message/content
            content = ""
            prompt_tokens = completion_tokens = total_tokens = 0
            
            if isinstance(data, dict):
                # OpenAI-like
                choice = data.get("choices")
                if choice and isinstance(choice, list) and len(choice) > 0:
                    first = choice[0]
                    # nested message
                    msg = first.get("message") or {}
                    content = msg.get("content") or first.get("text") or ""
                    
                usage = data.get("usage")
                if usage:
                    prompt_tokens = int(usage.get("prompt_tokens", 0))
                    completion_tokens = int(usage.get("completion_tokens", 0))
                    total_tokens = int(usage.get("total_tokens", prompt_tokens + completion_tokens))
            else:
                # 非标准结构，尝试字符串化返回
                content = str(data)
                
            return ChatResponse(
                content=content,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
            )
                
        except httpx.HTTPStatusError as e:
            # 对 429/5xx 做重试（teancity 会重试），同时记录详细信息
            logger.error(f"LLM HTTP error status={e.response.status_code}: {e.response.text}")
            raise        
        except httpx.RequestError as e:
            logger.error(f"LLM Request error: {e}", exc_info=True)    
            raise
        except Exception as e:
            logger.error(f"Unexcepted LLM client error: {e}", exc_info=True)
            raise
        
async def get_llm_client() -> LLMClient:
    llm_client = LLMClient()
    return llm_client