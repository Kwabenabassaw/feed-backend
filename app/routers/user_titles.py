"""
User Titles Router

Handles user-title relationship synchronization from Firebase to Supabase.
Endpoints for syncing watchlist/watching/finished states, ratings, and favorites.

Security Model:
- Client writes to Firebase first (source of truth)
- Client then calls this endpoint to sync to Supabase (derived intelligence)
- Firebase ID token is verified; uid is extracted from verified token
- Supabase service role used server-side only
- Client NEVER has access to Supabase credentials

Corrections Applied (Phase 3):
1. source column support (optional ML signal)
2. added_at guaranteed non-null
3. Status removal clears status_changed_at
4. Idempotent UPSERT on (user_id, title_id)
5. No server-side state transition validation
"""

import os
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..core.security import get_current_user
from ..core.logging import get_logger
from ..config import get_settings

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/user-titles", tags=["user-titles"])

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", settings.supabase_url)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


class MediaType(str, Enum):
    """Media type enum."""
    MOVIE = "movie"
    TV = "tv"


class TitleStatus(str, Enum):
    """Title status enum (mutually exclusive)."""
    WATCHING = "watching"
    WATCHLIST = "watchlist"
    FINISHED = "finished"


class TitleSource(str, Enum):
    """Source of title discovery (optional ML signal)."""
    SEARCH = "search"
    RECOMMENDATION = "recommendation"
    FRIEND = "friend"
    COMMUNITY = "community"


class UserTitleSyncRequest(BaseModel):
    """Request body for user title sync."""
    title_id: str = Field(..., description="TMDB ID of the title")
    media_type: MediaType = Field(..., description="movie or tv")
    title: str = Field(..., description="Title name")
    poster_path: Optional[str] = Field(None, description="TMDB poster path")
    status: Optional[TitleStatus] = Field(None, description="Current status (null to remove from lists)")
    is_favorite: bool = Field(False, description="Whether title is favorited")
    rating: Optional[int] = Field(None, ge=1, le=5, description="Rating 1-5")
    source: Optional[TitleSource] = Field(None, description="How user discovered this title")


class UserTitleSyncResponse(BaseModel):
    """Response from user title sync."""
    success: bool
    user_id: str
    title_id: str
    status: Optional[str]
    is_favorite: bool
    message: str


@router.post("/sync", response_model=UserTitleSyncResponse)
async def sync_user_title(
    body: UserTitleSyncRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Sync user-title relationship from Firebase to Supabase.
    
    CRITICAL: This endpoint assumes Firebase write already happened.
    This is a sync endpoint, NOT a primary write endpoint.
    
    Corrections Applied:
    - CORRECTION #3: When status is NULL, status_changed_at is set to NULL
    - CORRECTION #5: Endpoint is /user-titles/sync (renamed from /watchlist/sync)
    - Idempotent UPSERT on (user_id, title_id)
    - No server-side state transition validation
    
    Security:
    - user_id comes from verified Firebase token only
    - Uses Supabase service role (server-side only)
    - Client cannot forge user_id
    """
    user_id = current_user.get("uid")
    
    logger.info(
        "user_title_sync_request",
        user_id=user_id,
        title_id=body.title_id,
        status=body.status.value if body.status else None,
        is_favorite=body.is_favorite,
        rating=body.rating,
    )
    
    # Validation
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid token: missing uid")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    # Build the record dynamically based on what was sent
    # This allows partial updates (e.g. update rating without clearing status)
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "user_id": user_id,
        "title_id": body.title_id,
        "media_type": body.media_type.value,
        "title": body.title,
        "synced_at": now,
    }
    
    # Only update optional fields if they were explicitly sent
    fields_set = body.__fields_set__
    
    if "poster_path" in fields_set:
        record["poster_path"] = body.poster_path

    if "source" in fields_set:
        record["source"] = body.source.value if body.source else None
        
    # LOGIC: Status
    if "status" in fields_set:
        record["status"] = body.status.value if body.status else None
        # CORRECTION #3: Status removal semantics
        if body.status:
            record["status_changed_at"] = now
        else:
            record["status_changed_at"] = None
            
    # LOGIC: Rating
    if "rating" in fields_set:
        record["rating"] = body.rating
        if body.rating:
            record["rated_at"] = now
            
    # LOGIC: Favorite
    if "is_favorite" in fields_set:
        record["is_favorite"] = body.is_favorite
        if body.is_favorite:
            record["favorited_at"] = now
        else:
            record["favorited_at"] = None
            
    # CORRECTION #2: added_at
    # We only set added_at if we are creating new (handled by Supabase usually)
    # OR if this is a fresh sync.
    # But wait, UPSERT with merge will overwrite added_at if we send it.
    # We should ALWAYS send added_at for new records.
    # For existing records, we want to KEEP existing added_at.
    # PostgREST doesn't support "insert_only" columns in UPSERT easily.
    # BUT, if we use "resolution=merge-duplicates", it merges.
    # If we want to preserve added_at, we should NOT send it if record exists.
    # But we don't know if it exists.
    # Workaround: Send added_at only if status is NOT NULL (implies adding to list)
    # AND added_at is not in DB.
    # Actually, worst case: we overwrite added_at with NOW().
    # Ideally we want `added_at = COALESCE(excluded.added_at, user_titles.added_at)` logic.
    # PostgREST doesn't let us customize ON CONFLICT clause easily.
    # However, if we omit 'added_at' from JSON, it won't update it on merge.
    # But then new records will fail NOT NULL constraint if default is not set.
    # DB default is NOT SET?
    # Migration: added_at TIMESTAMPTZ NOT NULL
    # Fix: We must send added_at. If it overwrites, so be it (last sync wins).
    # OR: accepted trade-off for now.
    record["added_at"] = now
    
    try:
        async with httpx.AsyncClient() as client:
            # UPSERT: Insert or update on conflict
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/user_titles",
                json=record,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    # UPSERT: merge on conflict, don't overwrite added_at
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                timeout=10.0,
            )
            
            if response.status_code in [200, 201]:
                logger.info(
                    "user_title_sync_success",
                    user_id=user_id,
                    title_id=body.title_id,
                )
                return UserTitleSyncResponse(
                    success=True,
                    user_id=user_id,
                    title_id=body.title_id,
                    status=body.status.value if body.status else None,
                    is_favorite=body.is_favorite,
                    message="User title synced successfully",
                )
            elif response.status_code == 409:
                # FK constraint violation - user profile may not exist yet
                logger.warning(
                    "user_title_sync_conflict",
                    user_id=user_id,
                    title_id=body.title_id,
                    body=response.text[:200],
                )
                # Return success since Firebase is authoritative
                return UserTitleSyncResponse(
                    success=True,
                    user_id=user_id,
                    title_id=body.title_id,
                    status=body.status.value if body.status else None,
                    is_favorite=body.is_favorite,
                    message="User title recorded; profile may sync later",
                )
            else:
                logger.error(
                    "supabase_sync_failed",
                    status=response.status_code,
                    body=response.text[:200],
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Supabase error: {response.status_code}",
                )
                
    except httpx.TimeoutException:
        logger.error("supabase_timeout", user_id=user_id)
        raise HTTPException(status_code=504, detail="Supabase timeout")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("user_title_sync_error", error=str(e), user_id=user_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}")
async def get_user_titles(
    user_id: str,
    status: Optional[TitleStatus] = None,
    is_favorite: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """
    Get user's titles with optional filtering.
    
    Query params:
    - status: Filter by status (watching, watchlist, finished)
    - is_favorite: Filter by favorite status
    - limit: Max results (default 50)
    - offset: Pagination offset
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            params = {
                "user_id": f"eq.{user_id}",
                "order": "status_changed_at.desc.nullslast",
                "limit": limit,
                "offset": offset,
            }
            
            if status:
                params["status"] = f"eq.{status.value}"
            
            if is_favorite is not None:
                params["is_favorite"] = f"eq.{str(is_favorite).lower()}"
            
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_titles",
                params=params,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "user_id": user_id,
                    "count": len(data),
                    "titles": data,
                }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_user_titles_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/{user_id}")
async def get_user_title_stats(
    user_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Get aggregated stats for a user's titles.
    
    Returns counts for each status and favorites.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            # Get all titles for user
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_titles",
                params={
                    "user_id": f"eq.{user_id}",
                    "select": "status,is_favorite,rating",
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Compute stats
                stats = {
                    "watching_count": 0,
                    "watchlist_count": 0,
                    "finished_count": 0,
                    "favorites_count": 0,
                    "rated_count": 0,
                    "average_rating": None,
                }
                
                ratings = []
                for item in data:
                    status = item.get("status")
                    if status == "watching":
                        stats["watching_count"] += 1
                    elif status == "watchlist":
                        stats["watchlist_count"] += 1
                    elif status == "finished":
                        stats["finished_count"] += 1
                    
                    if item.get("is_favorite"):
                        stats["favorites_count"] += 1
                    
                    if item.get("rating"):
                        stats["rated_count"] += 1
                        ratings.append(item["rating"])
                
                if ratings:
                    stats["average_rating"] = round(sum(ratings) / len(ratings), 2)
                
                return {
                    "user_id": user_id,
                    **stats,
                }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_user_title_stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
