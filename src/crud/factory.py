from typing import Dict, Type, Any
from threading import Lock

from src.crud.chat import ChatCRUD
from src.crud.document import DocumentCRUD


"""
DAO 通常不使用单例模式：
所有请求共享同一个 DAO，也就共享了同一个 session 可能会导致事务混乱、并发错误！
正确做法：在请求或任务上下文中，用 session 实例化 DAO，并通过依赖注入传递。
"""
# class DAOFactory:
#     """
#     统一管理所有 DAO 实例的创建和生命周期
#     支持单例、懒加载、依赖注入
#     """
    
#     # 存储已创建的实例（单例缓存）
#     _instances: Dict[str, Any] = {}
    
#     # 数据库连接（可以是连接池）
#     _db_connection = None
    
#     # 线程锁，保证线程安全
#     _lock = Lock()

#     @classmethod
#     def set_connection(cls, conn):
#         """设置数据库连接（由应用启动时调用）"""
#         cls._db_connection = conn

#     @classmethod
#     def get_dao(cls, dao_class: Type, *args, **kwargs):
#         """
#         通用方法：根据类获取 DAO 实例（支持任意 DAO）
#         自动使用单例模式
#         """
#         # 生成唯一 key（如 'UserDAO'）
#         key = dao_class.__name__
        
#         if key not in cls._instances:
#             with cls._lock:  # 线程安全
#                 if key not in cls._instances:  # double-check
#                     # 自动注入数据库连接（如果 __init__ 需要）
#                     if not args and not kwargs and hasattr(dao_class, '__init__'):
#                         instance = dao_class(db_connection=cls._db_connection)
#                     else:
#                         instance = dao_class(*args, **kwargs)
#                     cls._instances[key] = instance
#         return cls._instances[key]

#     # 以下是快捷方法（可选，提升可读性）
#     @classmethod
#     def get_chat_dao(cls):
#         return cls.get_dao(ChatCRUD)
    
#     @classmethod
#     def get_document_dao(cls):
#         return cls.get_dao(DocumentCRUD)