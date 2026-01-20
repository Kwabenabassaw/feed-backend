"""
Social Router

Handles social graph synchronization from Firebase to Supabase.
Endpoints for follow/unfollow sync and social queries.

Security Model:
- Client writes follow/unfollow to Firebase first (source of truth)
- Client then calls this endpoint to sync to Supabase (derived intelligence)
- Firebase ID token is verified; uid is extracted from verified token
- Supabase service role used server-side only
- Client NEVER has access to Supabase credentials

Idempotency:
- Follow: UPSERT (safe to retry)
- Unfollow: DELETE (safe if row doesn't exist)
"""

import os
import httpx
from enum import Enum
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..core.security import get_current_user
from ..core.logging import get_logger
from ..config import get_settings

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/social", tags=["social"])

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", settings.supabase_url)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


class FollowAction(str, Enum):
    """Enum for follow actions."""
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"


class FollowRequest(BaseModel):
    """Request body for follow/unfollow sync."""
    target_uid: str = Field(..., description="Firebase UID of target user")
    action: FollowAction = Field(..., description="follow or unfollow")


class FollowResponse(BaseModel):
    """Response from follow/unfollow sync."""
    success: bool
    action: str
    follower_uid: str
    target_uid: str
    message: str


class FollowersResponse(BaseModel):
    """Response from get followers/following."""
    user_id: str
    count: int
    users: list


@router.post("/follow", response_model=FollowResponse)
async def sync_follow(
    body: FollowRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Sync follow/unfollow action from Firebase to Supabase.
    
    CRITICAL: This endpoint assumes Firebase write already happened.
    This is a sync endpoint, NOT a primary write endpoint.
    
    Flow:
    1. Verify Firebase token (done by get_current_user dependency)
    2. Extract uid from verified token (NEVER trust client-sent uid)
    3. Validate request (no self-follow, valid action)
    4. UPSERT or DELETE from Supabase follows table
    5. Triggers automatically update user_stats counts
    
    Idempotency:
    - Follow: UPSERT with ON CONFLICT DO NOTHING (safe to retry)
    - Unfollow: DELETE WHERE (safe if row doesn't exist)
    
    Security:
    - uid comes from verified Firebase token only
    - Uses Supabase service role (server-side only)
    - Client cannot forge follower_id
    """
    follower_uid = current_user.get("uid")
    target_uid = body.target_uid
    action = body.action
    
    logger.info(
        "follow_sync_request",
        follower_uid=follower_uid,
        target_uid=target_uid,
        action=action.value,
    )
    
    # Validation
    if not follower_uid:
        raise HTTPException(status_code=400, detail="Invalid token: missing uid")
    
    if follower_uid == target_uid:
        raise HTTPException(status_code=400, detail="Cannot follow yourself")
    
    if not target_uid:
        raise HTTPException(status_code=400, detail="Missing target_uid")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            if action == FollowAction.FOLLOW:
                # UPSERT: Insert or do nothing if already exists
                response = await client.post(
                    f"{SUPABASE_URL}/rest/v1/follows",
                    json={
                        "follower_id": follower_uid,
                        "following_id": target_uid,
                    },
                    headers={
                        "apikey": SUPABASE_SERVICE_KEY,
                        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                        "Content-Type": "application/json",
                        # On conflict (composite PK), ignore duplicate
                        "Prefer": "resolution=ignore-duplicates,return=minimal",
                    },
                    timeout=10.0,
                )
                
                # 201 = created, 200 = already existed (ignored)
                if response.status_code in [200, 201]:
                    logger.info(
                        "follow_sync_success",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                    )
                    return FollowResponse(
                        success=True,
                        action="follow",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                        message="Follow synced successfully",
                    )
                elif response.status_code == 409:
                    # Conflict - likely FK constraint violation
                    # Target user may not exist in profiles table yet
                    logger.warning(
                        "follow_sync_conflict",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                        body=response.text[:200],
                    )
                    # Return success=True since Firebase is authoritative
                    # The sync will succeed once the target user syncs their profile
                    return FollowResponse(
                        success=True,
                        action="follow",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                        message="Follow recorded; target profile may sync later",
                    )
                else:
                    logger.error(
                        "supabase_follow_failed",
                        status=response.status_code,
                        body=response.text[:200],
                    )
                    raise HTTPException(
                        status_code=500,
                        detail=f"Supabase error: {response.status_code}",
                    )
            
            else:  # UNFOLLOW
                # DELETE: Safe if row doesn't exist
                response = await client.delete(
                    f"{SUPABASE_URL}/rest/v1/follows",
                    params={
                        "follower_id": f"eq.{follower_uid}",
                        "following_id": f"eq.{target_uid}",
                    },
                    headers={
                        "apikey": SUPABASE_SERVICE_KEY,
                        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    },
                    timeout=10.0,
                )
                
                # 200 or 204 = success (deleted or didn't exist)
                if response.status_code in [200, 204]:
                    logger.info(
                        "unfollow_sync_success",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                    )
                    return FollowResponse(
                        success=True,
                        action="unfollow",
                        follower_uid=follower_uid,
                        target_uid=target_uid,
                        message="Unfollow synced successfully",
                    )
                else:
                    logger.error(
                        "supabase_unfollow_failed",
                        status=response.status_code,
                        body=response.text[:200],
                    )
                    raise HTTPException(
                        status_code=500,
                        detail=f"Supabase error: {response.status_code}",
                    )
                    
    except httpx.TimeoutException:
        logger.error("supabase_timeout", follower_uid=follower_uid)
        raise HTTPException(status_code=504, detail="Supabase timeout")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("follow_sync_error", error=str(e), follower_uid=follower_uid)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/followers/{user_id}")
async def get_followers(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """
    Get followers of a user from Supabase.
    
    This is a read-only query endpoint for analytics and intelligence.
    For realtime UX, clients should still use Firebase.
    """
    logger.info("get_followers_request", user_id=user_id, limit=limit)
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            # Query follows table with JOIN to profiles
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/follows",
                params={
                    "following_id": f"eq.{user_id}",
                    "select": "follower_id,created_at",
                    "order": "created_at.desc",
                    "limit": limit,
                    "offset": offset,
                },
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
                    "followers": data,
                }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_followers_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/following/{user_id}")
async def get_following(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """
    Get users that a user follows from Supabase.
    
    This is a read-only query endpoint for analytics and intelligence.
    For realtime UX, clients should still use Firebase.
    """
    logger.info("get_following_request", user_id=user_id, limit=limit)
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/follows",
                params={
                    "follower_id": f"eq.{user_id}",
                    "select": "following_id,created_at",
                    "order": "created_at.desc",
                    "limit": limit,
                    "offset": offset,
                },
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
                    "following": data,
                }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_following_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/{user_id}")
async def get_user_stats(
    user_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Get follower/following counts from cached user_stats table.
    
    Counts are maintained automatically by database triggers.
    """
    logger.info("get_stats_request", user_id=user_id)
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_stats",
                params={
                    "user_id": f"eq.{user_id}",
                    "select": "followers_count,following_count,updated_at",
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    return {
                        "user_id": user_id,
                        "followers_count": data[0]["followers_count"],
                        "following_count": data[0]["following_count"],
                        "updated_at": data[0]["updated_at"],
                    }
                else:
                    # User has no stats yet (no follows)
                    return {
                        "user_id": user_id,
                        "followers_count": 0,
                        "following_count": 0,
                        "updated_at": None,
                    }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mutuals/{user_id}")
async def get_mutual_follows(
    user_id: str,
    limit: int = 20,
    current_user: dict = Depends(get_current_user),
):
    """
    Get mutual follows (users who follow each other).
    
    This query finds users where:
    - user_id follows them AND
    - they follow user_id
    """
    logger.info("get_mutuals_request", user_id=user_id, limit=limit)
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            # Use RPC for complex query or build with subselect
            # For simplicity, we'll do two queries and intersect
            following_response = await client.get(
                f"{SUPABASE_URL}/rest/v1/follows",
                params={
                    "follower_id": f"eq.{user_id}",
                    "select": "following_id",
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            followers_response = await client.get(
                f"{SUPABASE_URL}/rest/v1/follows",
                params={
                    "following_id": f"eq.{user_id}",
                    "select": "follower_id",
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            if following_response.status_code == 200 and followers_response.status_code == 200:
                following_ids = {f["following_id"] for f in following_response.json()}
                follower_ids = {f["follower_id"] for f in followers_response.json()}
                
                mutual_ids = list(following_ids.intersection(follower_ids))[:limit]
                
                return {
                    "user_id": user_id,
                    "count": len(mutual_ids),
                    "mutual_ids": mutual_ids,
                }
            else:
                logger.error("supabase_query_failed")
                raise HTTPException(status_code=500, detail="Query failed")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_mutuals_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
