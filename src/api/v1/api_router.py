from fastapi import APIRouter
from src.api.v1 import auth, users, documents
from src.api.v1 import document_jobs
from src.api.v1 import tasks
from src.api.v1 import chat
from src.api.v1 import sessions
from src.api.v1 import admin

api_router = APIRouter(prefix="/v1")

api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, tags=["users"])
api_router.include_router(documents.router, tags=["documents"])
api_router.include_router(document_jobs.router, tags=["document_jobs"])
api_router.include_router(tasks.router, tags=["tasks"])
api_router.include_router(chat.router, tags=["chat"])
api_router.include_router(sessions.router, tags=["sessions"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])