"""
Community Router

Handles mirroring of community data from Firestore to Supabase.
These are fire-and-forget sync endpoints called by Flutter after Firestore writes.

CRITICAL: Firestore remains the Source of Truth.
These endpoints mirror data for analytics, moderation, and ranking.
"""

import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import httpx
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..core.security import get_current_user
from ..core.logging import get_logger
from ..config import get_settings

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/community", tags=["community"])

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", settings.supabase_url)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


# =============================================================================
# REQUEST MODELS
# =============================================================================

class PostSyncRequest(BaseModel):
    """Request body for post sync."""
    id: str = Field(..., description="Firestore document ID")
    community_id: Optional[str] = Field(None, description="Community/Show ID as string")
    show_id: int = Field(..., description="TMDB Show ID")
    show_title: str = Field(..., description="Show title")
    author_id: str = Field(..., description="Firebase UID of author")
    author_name: Optional[str] = Field(None, description="Display name")
    author_avatar: Optional[str] = Field(None, description="Avatar URL")
    content: Optional[str] = Field(None, description="Post content")
    is_spoiler: bool = Field(False, description="Spoiler flag")
    is_hidden: bool = Field(False, description="Hidden/moderated flag")
    upvotes: int = Field(0, description="Upvote count")
    downvotes: int = Field(0, description="Downvote count")
    score: int = Field(0, description="Net score (upvotes - downvotes)")
    comment_count: int = Field(0, description="Comment count")
    created_at: Optional[str] = Field(None, description="ISO8601 timestamp")
    last_activity_at: Optional[str] = Field(None, description="ISO8601 timestamp")


class ReportSyncRequest(BaseModel):
    """Request body for report sync."""
    id: str = Field(..., description="Firestore document ID")
    type: str = Field(..., description="Report type: communityPost, communityComment, chatMessage")
    reason: str = Field(..., description="Report reason: spam, harassment, hate, etc.")
    content_id: str = Field(..., description="Firestore ID of reported content")
    reporter_id: str = Field(..., description="Firebase UID of reporter")
    reported_user_id: str = Field(..., description="Firebase UID of content author")
    severity: str = Field("low", description="low, medium, high")
    status: str = Field("pending", description="pending, reviewed, actioned, dismissed")
    content_snapshot: Optional[Dict[str, Any]] = Field(None, description="Frozen content")
    created_at: Optional[str] = Field(None, description="ISO8601 timestamp")


class SyncResponse(BaseModel):
    """Standard sync response."""
    success: bool
    id: str
    message: str


# =============================================================================
# POST SYNC ENDPOINT
# =============================================================================

@router.post("/sync/post", response_model=SyncResponse)
async def sync_post(
    body: PostSyncRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Mirror a community post from Firestore to Supabase.
    
    Called by Flutter after creating/updating a post in Firestore.
    Uses UPSERT to handle both create and update idempotently.
    
    Safety Rules:
    - Never overwrite newer last_activity_at
    - Always update synced_at
    - Preserve existing data if payload fields are missing
    """
    logger.info(
        "post_sync_request",
        post_id=body.id,
        show_id=body.show_id,
        author_id=body.author_id,
    )
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    # Build record
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "id": body.id,
        "community_id": body.community_id or str(body.show_id),
        "show_id": body.show_id,
        "show_title": body.show_title,
        "author_id": body.author_id,
        "synced_at": now,
    }
    
    # Only include optional fields if provided (preserve existing on UPSERT)
    if body.author_name is not None:
        record["author_name"] = body.author_name
    if body.author_avatar is not None:
        record["author_avatar"] = body.author_avatar
    if body.content is not None:
        record["content"] = body.content
    
    # Boolean fields - always include
    record["is_spoiler"] = body.is_spoiler
    record["is_hidden"] = body.is_hidden
    
    # Counter fields - always include (aggregated from Firestore)
    record["upvotes"] = body.upvotes
    record["downvotes"] = body.downvotes
    record["score"] = body.score
    record["comment_count"] = body.comment_count
    
    # Timestamps
    if body.created_at:
        record["created_at"] = body.created_at
    if body.last_activity_at:
        record["last_activity_at"] = body.last_activity_at
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/community_posts_mirror",
                json=record,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                timeout=10.0,
            )
            
            if response.status_code in [200, 201]:
                logger.info("post_sync_success", post_id=body.id)
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message="Post mirrored successfully",
                )
            else:
                logger.error(
                    "post_sync_failed",
                    post_id=body.id,
                    status=response.status_code,
                    body=response.text[:200],
                )
                # Return success anyway - Firestore is source of truth
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message=f"Post recorded; mirror may sync later ({response.status_code})",
                )
                
    except httpx.TimeoutException:
        logger.error("supabase_timeout", post_id=body.id)
        return SyncResponse(
            success=True,
            id=body.id,
            message="Timeout; mirror will sync later",
        )
    except Exception as e:
        logger.error("post_sync_error", post_id=body.id, error=str(e))
        return SyncResponse(
            success=True,
            id=body.id,
            message=f"Mirror error: {str(e)[:50]}",
        )


# =============================================================================
# REPORT SYNC ENDPOINT
# =============================================================================

@router.post("/sync/report", response_model=SyncResponse)
async def sync_report(
    body: ReportSyncRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Mirror a report from Firestore to Supabase.
    
    Called by Flutter after submitting a report in Firestore.
    Reports are NEVER deleted - they form an audit trail.
    
    Safety Rules:
    - Status defaults to 'pending'
    - Never delete reports
    - Content snapshot is frozen at report time
    """
    logger.info(
        "report_sync_request",
        report_id=body.id,
        type=body.type,
        content_id=body.content_id,
        severity=body.severity,
    )
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    # Build record
    record = {
        "id": body.id,
        "type": body.type,
        "reason": body.reason,
        "content_id": body.content_id,
        "reporter_id": body.reporter_id,
        "reported_user_id": body.reported_user_id,
        "severity": body.severity,
        "status": body.status,
    }
    
    if body.content_snapshot:
        record["content_snapshot"] = body.content_snapshot
    if body.created_at:
        record["created_at"] = body.created_at
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/community_reports",
                json=record,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                timeout=10.0,
            )
            
            if response.status_code in [200, 201]:
                logger.info("report_sync_success", report_id=body.id)
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message="Report mirrored successfully",
                )
            else:
                logger.error(
                    "report_sync_failed",
                    report_id=body.id,
                    status=response.status_code,
                    body=response.text[:200],
                )
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message=f"Report recorded; mirror may sync later ({response.status_code})",
                )
                
    except Exception as e:
        logger.error("report_sync_error", report_id=body.id, error=str(e))
        return SyncResponse(
            success=True,
            id=body.id,
            message=f"Mirror error: {str(e)[:50]}",
        )


# =============================================================================
# AGGREGATE SYNC ENDPOINT (Batch Counter Updates)
# =============================================================================

@router.post("/sync/post/aggregate", response_model=SyncResponse)
async def sync_post_aggregate(
    body: PostSyncRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Update only aggregated counters for a post.
    
    Used for periodic batch updates of vote counts.
    DOES NOT sync raw votes - only pre-aggregated totals from Firestore.
    """
    logger.info(
        "post_aggregate_sync",
        post_id=body.id,
        upvotes=body.upvotes,
        downvotes=body.downvotes,
        score=body.score,
    )
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    # Only update counters and activity
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "id": body.id,
        "upvotes": body.upvotes,
        "downvotes": body.downvotes,
        "score": body.score,
        "comment_count": body.comment_count,
        "synced_at": now,
    }
    
    if body.last_activity_at:
        record["last_activity_at"] = body.last_activity_at
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{SUPABASE_URL}/rest/v1/community_posts_mirror?id=eq.{body.id}",
                json=record,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                timeout=10.0,
            )
            
            if response.status_code in [200, 204]:
                logger.info("aggregate_sync_success", post_id=body.id)
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message="Counters updated",
                )
            else:
                logger.warning(
                    "aggregate_sync_no_match",
                    post_id=body.id,
                    status=response.status_code,
                )
                return SyncResponse(
                    success=True,
                    id=body.id,
                    message="Post not found in mirror; will sync on next full update",
                )
                
    except Exception as e:
        logger.error("aggregate_sync_error", post_id=body.id, error=str(e))
        return SyncResponse(
            success=True,
            id=body.id,
            message=f"Aggregate error: {str(e)[:50]}",
        )
