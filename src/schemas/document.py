from pydantic import BaseModel, Field, computed_field
from pydantic import field_validator, model_validator
from typing import Optional, List, Generic, TypeVar
from uuid import UUID
# from enum import Enum as PyEnum
from datetime import datetime
from math import ceil

from src.models.document import DocumentStatus
from src.utils.file_validator import sanitize_filename



class DocumentBase(BaseModel):
    """
    所有 Document 模型的基类
    包含核心字段，用于继承
    """
    filename: str = Field(
        ...,  # 必填，没有默认值，调用 schema 时必须传。
        min_length=1,
        max_length=255,
        description="原始文件名"
    )
    
    content_type: Optional[str] = Field(
        default=None,
        max_length=100,
        description="MIME 类型，如 application/pdf"
    )
    
    @field_validator("filename")
    @classmethod
    def validate_filename(cls, v: str) -> str:
        return sanitize_filename(v)                


class DocumentCreate(DocumentBase):
    """
    创建文档时的输入模型
    通常由上传接口接收

    注意：以下字段由后端自动生成，不接受前端输入：
    - storage_key
    - size_bytes
    - checksum
    - status
    - user_id
    """
    pass  # 继承 DocumentBase 即可 

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": [
                {
                    "filename": "example.pdf", 
                    "content_type": "application/pdf"
                }
            ]
        }
    }


class DocumentUpdate(BaseModel):
    """
    更新文档时的输入模型（可选字段）
    用于 PATCH /documents/{id} 接口
    """
    filename: Optional[str] = Field(
        None,  # 可选，默认为 None，调用 schema 时可以不传。
        min_length=1,
        max_length=255,
    )
    
    content_type: Optional[str] = Field(None, max_length=100)
    
    # Pydantic v2 的模型级验证器，mode="after" 表示在字段级校验之后执行
    @model_validator(mode="after")
    def check_at_least_one_value(self):
        if not any(getattr(self, field) is not None for field in self.__class__.model_fields):
            raise ValueError("至少提供一个字段进行更新")
        return self
    
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": [
                {
                    "filename": "updated_example.pdf"
                }
            ]
        }
    }
    

class DocumentResponse(DocumentBase):
    """
    返回单个文档信息的响应模型
    注意：该模型不包含敏感字段（如 storage_key, checksum, user_id）
    """
    
    id: UUID = Field(..., description="文档 UUID")  # UUID 转换为 str
    size_bytes: int = Field(ge=0, description="文件大小，单位字节")
    status: DocumentStatus = Field(..., description="文档处理状态")
    error_message: Optional[str] = Field(None, description="错误信息（仅当失败时）")
    created_at: datetime = Field(..., description="创建时间（UTC）")
    updated_at: datetime = Field(..., description="最后更新时间（UTC）")
    
    model_config = {
        "from_attributes": True,  # 支持 ORM 模型直接转换为该模型
        "json_schema_extra": {
            "example": [
                {
                    "id": "a1b2c3d4-1234-5678-90ab-cdef12345678",
                    "filename": "example.pdf",
                    "size_bytes": 12345,
                    "content_type": "application/pdf",
                    "status": "indexed",
                    "error_message": None,
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                } 
            ]
        }
    }
 
       
# 通用分页响应模型
T = TypeVar('T')  # 用于泛型，支持不同类型的分页数据

class PaginationResponse(BaseModel, Generic[T]):
    """
    通用分页响应模型
    使用示例: PaginationResponse[DocumentResponse]
    """
    items: List[T] = Field(
        default_factory=list, 
        description="当前页的数据列表",
        examples=[["示例数据"]]
    )
    total: int = Field(..., ge=0, description="总记录数")
    page: int = Field(..., ge=1, description="当前页码（从1开始）")
    size: int = Field(..., ge=1, le=100, description="每页记录数")
    
    @computed_field(description="总页数")  # 动态计算，避免输入错误
    @property
    def pages(self) -> int:
        if self.total == 0:
            return 0
        return int(ceil(self.total /self.size))
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "items": [
                        {
                            "id": "a1b2c3d4-1234-5678-90ab-cdef12345678",
                            "filename": "example.pdf",
                            "size_bytes": 12345,
                            "content_type": "application/pdf",
                            "status": "indexed",
                            "error_message": None,
                            "created_at": "2023-01-01T00:00:00Z",
                            "updated_at": "2023-01-01T00:00:00Z",
                        }
                    ],
                    "total": 1,
                    "page": 1,
                    "size": 10,
                    # "pages": 1,  # 自动计算，无需手动提供
                }   
            ]
        },
        "extra": "forbid",  # 严格模式，禁止额外字段
        "validate_default": True,  # 验证默认值
    }
    
    def model_post_init(self, __context):
        """
        可选：用于调试或日志
        """
        pass
    
def create_pagination_response(
    items: List[T],
    total: int,
    page: int,
    size: int,
) -> PaginationResponse[T]:
    
    return PaginationResponse[T](
        items=items,
        total=total,
        page=page,
        size=size,
        # pages 自动计算
    )