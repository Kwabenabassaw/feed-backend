"""
Search API Router

Endpoints for searching the curated content index.
"""

from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from ..config import get_settings
from ..core.logging import get_logger
from ..core.security import get_current_user_optional
from ..services.search_service import get_search_service

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/search", tags=["search"])
limiter = Limiter(key_func=get_remote_address)


@router.get("", response_model=List[Dict[str, Any]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
async def search_content(
    request: Request,
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(20, ge=1, le=50, description="Max results"),
    type: Optional[str] = Query(None, description="Filter by 'movie' or 'tv'"),
    current_user: Optional[dict] = Depends(get_current_user_optional)
):
    """
    Search for movies and TV shows in the feed index.
    
    Performs fast, in-memory fuzzy search over curated content.
    Returns list of feed items.
    """
    search_service = get_search_service()
    
    # Log search (useful for analytics later)
    uid = current_user["uid"] if current_user else "anonymous"
    logger.info("search_request", uid=uid, query=q, type=type)
    
    results = await search_service.search(
        query=q,
        limit=limit,
        media_type=type
    )
    
    return results
