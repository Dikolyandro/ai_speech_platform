from celery import Celery
from app.core.config import settings

celery_client = Celery(
    "backend_client",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)
