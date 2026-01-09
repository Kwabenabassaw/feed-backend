"""
Feed API Router

Main endpoint for fetching personalized feed.
Target: < 150ms latency
"""

import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from ..config import get_settings
from ..core.logging import get_logger
from ..core.security import get_current_user, get_current_user_optional
from ..models.response import FeedResponse, FeedMeta, FeedType
from ..models.user import UserContext, UserPreferences
from ..services.index_pool import IndexPoolService
from ..services.deduplication import DeduplicationService
from ..services.generator import FeedGenerator
from ..services.hydrator import Hydrator
from ..services.firestore_service import get_firestore_service

logger = get_logger(__name__)
settings = get_settings()

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

router = APIRouter(prefix="/feed", tags=["feed"])

# Singleton instances (initialized on first request)
_index_pool: Optional[IndexPoolService] = None
_dedup_service: Optional[DeduplicationService] = None
_generator: Optional[FeedGenerator] = None
_hydrator: Optional[Hydrator] = None


def get_services():
    """Get or initialize service singletons."""
    global _index_pool, _dedup_service, _generator, _hydrator
    
    if _index_pool is None:
        _index_pool = IndexPoolService()
        _dedup_service = DeduplicationService()
        _generator = FeedGenerator(_index_pool, _dedup_service)
        _hydrator = Hydrator()
    
    return _index_pool, _dedup_service, _generator, _hydrator


async def load_user_context(user: Optional[dict]) -> UserContext:
    """
    Load full user context from Firestore.
    
    Fetches:
    - User preferences (genres, providers)
    - Friend list
    - Seen history (for deduplication)
    - Favorites and watchlist (for personalization)
    
    For anonymous users, returns default context.
    """
    if user is None:
        # Return cold-start context for anonymous users
        return UserContext(
            user_id="anonymous",
            preferences=UserPreferences(),
            friend_ids=[],
            seen_item_ids=set(),
        )
    
    firestore = get_firestore_service()
    return await firestore.load_user_context(user["uid"])


@router.get("", response_model=FeedResponse)
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
async def get_feed(
    request: Request,
    feed_type: FeedType = Query(FeedType.FOR_YOU, description="Type of feed"),
    cursor: Optional[str] = Query(None, description="Pagination cursor"),
    limit: int = Query(10, ge=1, le=50, description="Number of items"),
    current_user: Optional[dict] = Depends(get_current_user_optional)  # Optional auth
):
    """
    Get personalized feed.
    
    Flow:
    1. Load user context (preferences, friends, history)
    2. Generator: Select item IDs using 50/30/20 mixing
    3. Deduplication: Filter seen items via session tracking
    4. Hydrator: Fetch full metadata for selected IDs
    5. Return response with next cursor
    
    Target latency: < 150ms
    
    Note: Works for both authenticated and anonymous users.
    Anonymous users receive trending/cold-start content.
    """
    start_time = time.time()
    
    _, _, generator, hydrator = get_services()
    
    user_id = current_user["uid"] if current_user else "anonymous"
    
    logger.info(
        "feed_request",
        uid=user_id,
        feed_type=feed_type.value,
        limit=limit,
        has_cursor=cursor is not None
    )
    
    try:
        # Step 1: Load user context
        user_context = await load_user_context(current_user)
        
        # Step 2 & 3: Generate and deduplicate
        selected_ids, next_cursor = await generator.generate(
            user_context=user_context,
            limit=limit,
            cursor=cursor
        )
        
        # Step 4: Hydrate with full metadata
        feed_items = await hydrator.hydrate(selected_ids)
        
        # Calculate latency
        latency_ms = int((time.time() - start_time) * 1000)
        
        # Build response
        response = FeedResponse(
            feed=feed_items,
            meta=FeedMeta(
                feedType=feed_type,
                page=1,  # TODO: Calculate from cursor
                limit=limit,
                itemCount=len(feed_items),
                hasMore=len(feed_items) == limit,
                generatedAt=datetime.utcnow(),
                latencyMs=latency_ms,
                cursor=next_cursor
            )
        )
        
        logger.info(
            "feed_response",
            uid=current_user["uid"],
            items=len(feed_items),
            latency_ms=latency_ms
        )
        
        return response
        
    except Exception as e:
        logger.error("feed_error", uid=current_user["uid"], error=str(e))
        raise


@router.get("/health")
async def health_check():
    """Health check endpoint (no auth required)."""
    return {
        "status": "healthy",
        "service": "feed-backend",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/quotas")
async def get_quotas(current_user: dict = Depends(get_current_user)):
    """Get current API quota status (admin only in production)."""
    from ..services.quota_manager import QuotaManager
    
    quota_manager = QuotaManager()
    return await quota_manager.get_all_quotas()
