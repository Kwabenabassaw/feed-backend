"""
Scheduler API Router

Endpoints for monitoring and manually triggering background jobs.
Supports both Firebase auth and API key authentication for admin operations.
"""

import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header

from ..core.logging import get_logger
from ..core.security import get_current_user_optional
from ..services.scheduler import get_scheduler_service
from ..services.supabase_storage import get_supabase_storage

logger = get_logger(__name__)

router = APIRouter(prefix="/scheduler", tags=["scheduler"])

# Admin API key from environment (for cron jobs and manual triggers)
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "finishd-admin-2026")


async def verify_admin_access(
    current_user: Optional[dict] = Depends(get_current_user_optional),
    x_api_key: Optional[str] = Header(None),
):
    """
    Verify admin access via Firebase auth OR API key.
    
    Accepts:
    - Firebase auth token: Authorization: Bearer <token>
    - API key: X-API-Key: <admin_key>
    """
    # Option 1: Firebase auth
    if current_user is not None:
        logger.info("admin_access_firebase", uid=current_user.get("uid"))
        return {"method": "firebase", "uid": current_user.get("uid")}
    
    # Option 2: API key
    if x_api_key and x_api_key == ADMIN_API_KEY:
        logger.info("admin_access_api_key")
        return {"method": "api_key", "uid": "admin"}
    
    raise HTTPException(
        status_code=401,
        detail="Missing or invalid authentication. Use Firebase token or X-API-Key header."
    )


@router.get("/status")
async def get_scheduler_status(admin: dict = Depends(verify_admin_access)):
    """
    Get current scheduler status and job information.
    
    Returns:
        - Whether scheduler is running
        - List of jobs with next run times
    """
    service = get_scheduler_service()
    return service.get_job_status()


@router.post("/trigger/ingestion")
async def trigger_ingestion(admin: dict = Depends(verify_admin_access)):
    """
    Manually trigger the content ingestion job.
    
    Fetches content from YouTube RSS and TMDB.
    """
    logger.info("manual_ingestion_trigger", admin=admin)
    
    service = get_scheduler_service()
    await service.trigger_ingestion_now()
    
    return {
        "success": True,
        "message": "Ingestion job triggered",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/trigger/indexer")
async def trigger_indexer(admin: dict = Depends(verify_admin_access)):
    """
    Manually trigger the index generation job.
    
    Regenerates all indices and uploads to Supabase.
    """
    logger.info("manual_indexer_trigger", admin=admin)
    
    service = get_scheduler_service()
    await service.trigger_indexer_now()
    
    return {
        "success": True,
        "message": "Indexer job triggered and uploaded",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/trigger/upload")
async def trigger_upload(admin: dict = Depends(verify_admin_access)):
    """
    Manually upload all indices to Supabase Storage.
    
    Useful after running seed_data.py locally.
    """
    logger.info("manual_upload_trigger", admin=admin)
    
    storage = get_supabase_storage()
    result = await storage.upload_all_indices()
    
    return {
        "success": result.get("failed", 0) == 0,
        "uploaded": result.get("success", 0),
        "failed": result.get("failed", 0),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/start")
async def start_scheduler(admin: dict = Depends(verify_admin_access)):
    """Start the background scheduler."""
    service = get_scheduler_service()
    service.start()
    return {"success": True, "message": "Scheduler started"}


@router.post("/stop")
async def stop_scheduler(admin: dict = Depends(verify_admin_access)):
    """Stop the background scheduler."""
    service = get_scheduler_service()
    service.stop()
    return {"success": True, "message": "Scheduler stopped"}

