from fastapi import APIRouter, Depends
from src.schemas.user import UserResponse
from src.models.user import User
from src.core.depends import get_current_user


router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)):
    """获取当前登录用户信息"""
    return current_user