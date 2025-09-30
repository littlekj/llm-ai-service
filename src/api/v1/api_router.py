from fastapi import APIRouter
from src.api.v1 import auth, users, documents
from src.api.v1 import tasks

api_router = APIRouter(prefix="/v1")

api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, tags=["users"])
api_router.include_router(documents.router, tags=["documents"])
api_router.include_router(tasks.router, tags=["tasks"])