from typing import List, TypeVar, Type
from pydantic import BaseModel
from math import ceil


# 通用分页响应模型，用于泛型，支持不同类型的分页数据
T = TypeVar('T')

PaginationResponseT = TypeVar('PaginationResponseT', bound=BaseModel)


def create_pagination_response(
        items: List[T],
        total: int,
        page: int,
        size: int,
        response_model: Type[PaginationResponseT],
    ) -> PaginationResponseT:

        if size < 1:
            raise ValueError("size must be greater than 0")
        
        pages = max(1, ceil(total / size))  # 总是至少返回1页
        
        return response_model(
            items=items,
            total=total,
            page=page,
            size=size,
            pages=pages,
        )