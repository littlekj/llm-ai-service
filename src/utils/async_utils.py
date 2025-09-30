# utils/async_utils.py
import asyncio
import functools
from typing import Callable, Any, Coroutine
from asyncio import AbstractEventLoop
import threading


# 线程局部存储：每个线程有自己的事件循环
_thread_local = threading.local()

def _get_event_loop() -> AbstractEventLoop:
    """
    安全获取当前线程的事件循环，优先使用线程局部存储
    """
    # 优化从线程局部存储中获取事件循环
    if hasattr(_thread_local, "loop"):
        loop = _thread_local.loop
        if loop is not None and not loop.is_closed():
            return loop
        
    # 若当前线程无事件循环，则创建新的事件循环    
    loop = asyncio.new_event_loop()
    _thread_local.loop = loop
    asyncio.set_event_loop(loop)
    return loop
        
        
def run_in_async(func: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Any]:
    """
    装饰器：让同步函数调用 async 函数
    
    用法：
        @run_in_async
        async def my_async_task(x, y):
            await asyncio.sleep(1)
            return x + y
        result = my_async_task(1, 2)  # 同步调用，返回结果
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # 获取当前线程的专用事件循环
        loop = _get_event_loop()
        try:
            # 运行异步函数
            return loop.run_until_complete(func(*args, **kwargs))
        except Exception as e:
            # 捕获异常并抛出
            raise e
        finally:
            # 可选：关闭事件循环 loop（谨慎使用，可能影响性能）
            # 如果频繁使用，建议保持 loop 复用
            pass
        
    # 标记为已包装，便于调试
    setattr(wrapper, '_is_wrapped_async', True)
    return wrapper

# 或者直接函数
def sync_await(coroutine):
    return asyncio.run(coroutine)