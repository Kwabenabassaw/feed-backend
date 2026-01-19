"""API Routers."""

from .feed import router as feed_router
from .analytics import router as analytics_router
from .scheduler import router as scheduler_router
from .search import router as search_router
from .auth_sync import router as auth_sync_router

__all__ = [
    "feed_router",
    "analytics_router",
    "scheduler_router",
    "search_router",
    "auth_sync_router",
]

