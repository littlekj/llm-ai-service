"""
确保所有模型被导入，以便 Alembic 能扫描到 Base.metadata
"""
from src.models.base import Base
from src.models.user import User
from src.models.document import Document


# 可选：暴露常用模型
__all__ = ["Base", "User", "Document"]
