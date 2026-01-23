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
from ..core.security import get_current_user
from ..models.response import FeedResponse, FeedMeta, FeedType
from ..models.user import UserContext, UserPreferences
from ..services.index_pool import IndexPoolService
from ..services.deduplication import DeduplicationService
from ..services.generator import FeedGenerator
from ..services.hydrator import Hydrator
from ..services.firestore_service import get_firestore_service
from ..services.cache_service import get_redis_client

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
        # Inject Redis client for session management
        redis_client = get_redis_client()
        _dedup_service = DeduplicationService(redis_client=redis_client)
        _generator = FeedGenerator(_index_pool, _dedup_service)
        _hydrator = Hydrator()
    
    return _index_pool, _dedup_service, _generator, _hydrator


async def load_user_context(user: dict) -> UserContext:
    """
    Load full user context from Firestore.
    
    Fetches:
    - User preferences (genres, providers)
    - Friend list
    - Seen history (for deduplication)
    - Favorites and watchlist (for personalization)
    """
    firestore = get_firestore_service()
    return await firestore.load_user_context(user["uid"])


@router.get("", response_model=FeedResponse)
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
async def get_feed(
    request: Request,
    feed_type: FeedType = Query(FeedType.FOR_YOU, description="Type of feed"),
    cursor: Optional[str] = Query(None, description="Pagination cursor"),
    limit: int = Query(10, ge=1, le=100, description="Number of items"),
    current_user: dict = Depends(get_current_user)
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
    """
    start_time = time.time()
    
    _, _, generator, hydrator = get_services()
    
    logger.info(
        "feed_request",
        uid=current_user["uid"],
        feed_type=feed_type.value,
        limit=limit,
        has_cursor=cursor is not None
    )
    
    try:
        # --- SPECIAL PATH: Activity Feed (Following) ---
        if feed_type == FeedType.FOLLOWING:
            from ..services.social_service import get_social_service
            social_service = get_social_service()
            
            # Parse cursor (timestamp)
            cursor_dt = None
            if cursor:
                try:
                    cursor_dt = datetime.fromisoformat(cursor)
                except ValueError:
                    logger.warning("invalid_cursor_format", cursor=cursor)
            
            # Fetch from Supabase RPC
            activity_items = await social_service.get_activity_feed(
                user_id=current_user["uid"],
                limit=limit,
                cursor=cursor_dt
            )
            
            # Transform to Feed Response format
            feed_items = []
            last_activity_at = None
            
            for item in activity_items:
                # Map RPC result to API model
                feed_items.append({
                    "id": item.get("title_id"),
                    "mediaType": item.get("media_type"),
                    "title": item.get("title"),
                    "posterPath": item.get("poster_path"),
                    "rating": item.get("rating"),
                    "isFavorite": item.get("is_favorite"),
                    "status": item.get("status"),
                    "timestamp": item.get("activity_at"),
                    # Social Context
                    "friend": {
                        "uid": item.get("friend_user_id"),
                        "username": item.get("friend_username"),
                        "avatarUrl": item.get("friend_avatar_url"),
                    },
                    "type": "social_activity" 
                })
                last_activity_at = item.get("activity_at")

            # Determine next cursor
            next_cursor = None
            if len(activity_items) >= limit and last_activity_at:
                next_cursor = last_activity_at
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            response = FeedResponse(
                feed=feed_items,
                meta=FeedMeta(
                    feedType=feed_type,
                    page=1,
                    limit=limit,
                    itemCount=len(feed_items),
                    hasMore=len(feed_items) >= limit,
                    generatedAt=datetime.utcnow(),
                    latencyMs=latency_ms,
                    cursor=next_cursor
                )
            )
            return response

        # --- STANDARD PATH: For You / Trending ---
        # Step 1: Load user context
        user_context = await load_user_context(current_user)
        
        # Step 2 & 3: Generate and deduplicate
        selected_ids, next_cursor = await generator.generate(
            user_context=user_context,
            limit=limit,
            cursor=cursor,
            feed_type=feed_type.value
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
                hasMore=len(feed_items) >= limit,
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
