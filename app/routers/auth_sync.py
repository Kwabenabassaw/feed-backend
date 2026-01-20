"""
Auth Sync Router

Handles user profile synchronization from Firebase Auth to Supabase.
This enables the hybrid Firebase + Supabase architecture.

Security Model:
- Client sends Firebase ID token
- Backend verifies token (extracts uid, email)
- Backend uses Supabase service role to upsert profile
- Client NEVER has access to Supabase service credentials
"""

import os
import httpx
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from ..core.security import get_current_user
from ..core.logging import get_logger
from ..config import get_settings

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/auth", tags=["auth"])

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", settings.supabase_url)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


class ProfileSyncRequest(BaseModel):
    """Request body for profile sync."""
    username: Optional[str] = None
    profile_image: Optional[str] = None


class ProfileSyncResponse(BaseModel):
    """Response from profile sync."""
    success: bool
    uid: str
    message: str


@router.post("/sync-profile", response_model=ProfileSyncResponse)
async def sync_profile(
    body: ProfileSyncRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Sync user profile from Firebase to Supabase.
    
    Called by Flutter client after successful authentication.
    
    Flow:
    1. Verify Firebase token (done by get_current_user dependency)
    2. Extract uid and email from verified token
    3. Upsert into Supabase profiles table using service role
    
    Security:
    - Token already verified by Firebase Admin SDK
    - Uses Supabase service role (server-side only)
    - Client cannot forge uid
    """
    uid = current_user.get("uid")
    email = current_user.get("email", "")
    name = current_user.get("name", "")
    picture = current_user.get("picture", "")
    
    logger.info("profile_sync_request", uid=uid, email=email)
    
    if not uid:
        raise HTTPException(status_code=400, detail="Invalid token: missing uid")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("supabase_not_configured")
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        # Determine values to sync
        username = body.username or name or ""
        profile_image = body.profile_image or picture or ""
        
        # Upsert to Supabase using service role
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SUPABASE_URL}/rest/v1/profiles",
                json={
                    "id": uid,
                    "email": email,
                    "username": username,
                    "profile_image": profile_image,
                    # Note: created_at is set by database DEFAULT
                    # updated_at is set by database trigger on UPDATE
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    # UPSERT: on conflict with id, update existing row
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                timeout=10.0,
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info("profile_sync_success", uid=uid)
                return ProfileSyncResponse(
                    success=True,
                    uid=uid,
                    message="Profile synced successfully",
                )
            else:
                logger.error(
                    "supabase_upsert_failed",
                    status=response.status_code,
                    body=response.text[:200],
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Supabase error: {response.status_code}",
                )
    except httpx.TimeoutException:
        logger.error("supabase_timeout", uid=uid)
        raise HTTPException(status_code=504, detail="Supabase timeout")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("profile_sync_error", error=str(e), uid=uid)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profile-status")
async def get_profile_status(
    current_user: dict = Depends(get_current_user),
):
    """
    Check if user's profile exists in Supabase.
    
    Useful for:
    - Verifying sync completed
    - Checking if backfill is needed
    - Debugging sync issues
    """
    uid = current_user.get("uid")
    
    logger.info("profile_status_check", uid=uid)
    
    if not uid:
        raise HTTPException(status_code=400, detail="Invalid token: missing uid")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles",
                params={
                    "id": f"eq.{uid}",
                    "select": "id,email,username,profile_image,created_at,updated_at",
                },
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=5.0,
            )
            
            if response.status_code == 200:
                data = response.json()
                exists = len(data) > 0
                
                logger.info("profile_status_result", uid=uid, exists=exists)
                
                return {
                    "exists": exists,
                    "uid": uid,
                    "profile": data[0] if exists else None,
                }
            else:
                logger.error("supabase_query_failed", status=response.status_code)
                raise HTTPException(status_code=500, detail="Supabase query failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("profile_status_error", error=str(e), uid=uid)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/profile")
async def delete_profile(
    current_user: dict = Depends(get_current_user),
):
    """
    Delete user's profile from Supabase.
    
    Use case: Account deletion, GDPR compliance.
    """
    uid = current_user.get("uid")
    
    logger.info("profile_delete_request", uid=uid)
    
    if not uid:
        raise HTTPException(status_code=400, detail="Invalid token: missing uid")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{SUPABASE_URL}/rest/v1/profiles",
                params={"id": f"eq.{uid}"},
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                },
                timeout=10.0,
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info("profile_delete_success", uid=uid)
                return {
                    "success": True,
                    "uid": uid,
                    "message": "Profile deleted successfully",
                }
            else:
                logger.error("supabase_delete_failed", status=response.status_code)
                raise HTTPException(
                    status_code=500,
                    detail=f"Supabase delete failed: {response.status_code}",
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("profile_delete_error", error=str(e), uid=uid)
        raise HTTPException(status_code=500, detail=str(e))
