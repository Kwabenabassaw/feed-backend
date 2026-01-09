"""
Scheduler API Router

Endpoints for monitoring and manually triggering background jobs.
"""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from ..core.logging import get_logger
from ..core.security import get_current_user
from ..services.scheduler import get_scheduler_service
from ..services.supabase_storage import get_supabase_storage

logger = get_logger(__name__)

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.get("/status")
async def get_scheduler_status(current_user: dict = Depends(get_current_user)):
    """
    Get current scheduler status and job information.
    
    Returns:
        - Whether scheduler is running
        - List of jobs with next run times
    """
    service = get_scheduler_service()
    return service.get_job_status()


@router.post("/trigger/ingestion")
async def trigger_ingestion(current_user: dict = Depends(get_current_user)):
    """
    Manually trigger the content ingestion job.
    
    Fetches content from YouTube RSS and TMDB.
    """
    logger.info("manual_ingestion_trigger", uid=current_user["uid"])
    
    service = get_scheduler_service()
    await service.trigger_ingestion_now()
    
    return {
        "success": True,
        "message": "Ingestion job triggered",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/trigger/indexer")
async def trigger_indexer(current_user: dict = Depends(get_current_user)):
    """
    Manually trigger the index generation job.
    
    Regenerates all indices and uploads to Supabase.
    """
    logger.info("manual_indexer_trigger", uid=current_user["uid"])
    
    service = get_scheduler_service()
    await service.trigger_indexer_now()
    
    return {
        "success": True,
        "message": "Indexer job triggered and uploaded",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/trigger/upload")
async def trigger_upload(current_user: dict = Depends(get_current_user)):
    """
    Manually upload all indices to Supabase Storage.
    
    Useful after running seed_data.py locally.
    """
    logger.info("manual_upload_trigger", uid=current_user["uid"])
    
    storage = get_supabase_storage()
    result = await storage.upload_all_indices()
    
    return {
        "success": result.get("failed", 0) == 0,
        "uploaded": result.get("success", 0),
        "failed": result.get("failed", 0),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/start")
async def start_scheduler(current_user: dict = Depends(get_current_user)):
    """Start the background scheduler."""
    service = get_scheduler_service()
    service.start()
    return {"success": True, "message": "Scheduler started"}


@router.post("/stop")
async def stop_scheduler(current_user: dict = Depends(get_current_user)):
    """Stop the background scheduler."""
    service = get_scheduler_service()
    service.stop()
    return {"success": True, "message": "Scheduler stopped"}
