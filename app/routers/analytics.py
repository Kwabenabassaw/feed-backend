"""
Analytics API Router

Handles batched event tracking from mobile clients.
Write-heavy endpoint - designed to not block feed reads.
"""

from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, BackgroundTasks

from ..core.logging import get_logger
from ..core.security import get_current_user
from ..models.response import AnalyticsEvent, AnalyticsBatch, EventType
from ..services.firestore_service import get_firestore_service

logger = get_logger(__name__)

router = APIRouter(prefix="/analytics", tags=["analytics"])


async def process_events_async(user_id: str, events: List[AnalyticsEvent]):
    """
    Process events asynchronously to not block the response.
    
    Actions:
    1. Writes events to Firestore (analytics_events collection)
    2. Updates user's seen_items for VIEW events (deduplication)
    """
    logger.info(
        "processing_events",
        uid=user_id,
        count=len(events),
        types=[e.event_type.value for e in events]
    )
    
    # Count events by type
    view_count = sum(1 for e in events if e.event_type == EventType.VIEW)
    like_count = sum(1 for e in events if e.event_type == EventType.LIKE)
    
    # Convert events to dicts for Firestore
    event_dicts = [
        {
            "eventType": e.event_type.value,
            "itemId": e.item_id,
            "timestamp": e.timestamp,
            "durationWatched": e.duration_watched,
            "metadata": e.metadata,
        }
        for e in events
    ]
    
    # Save to Firestore
    firestore = get_firestore_service()
    await firestore.save_analytics_events(user_id, event_dicts)
    
    logger.info(
        "events_processed",
        uid=user_id,
        views=view_count,
        likes=like_count
    )


@router.post("/event")
async def track_events(
    batch: AnalyticsBatch,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """
    Track batched analytics events.
    
    Client batches events and sends every 30s to:
    - Save battery/network on mobile
    - Reduce server load
    
    Events are processed asynchronously to return fast.
    """
    user_id = current_user["uid"]
    
    logger.info(
        "analytics_batch_received",
        uid=user_id,
        event_count=len(batch.events),
        session_id=batch.session_id
    )
    
    # Process in background - don't block response
    background_tasks.add_task(
        process_events_async,
        user_id,
        batch.events
    )
    
    return {
        "success": True,
        "message": f"Received {len(batch.events)} events",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/view")
async def track_single_view(
    item_id: str,
    duration_watched: int = 0,
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Track a single view event.
    
    Convenience endpoint for immediate tracking (e.g., when user leaves app).
    """
    event = AnalyticsEvent(
        eventType=EventType.VIEW,
        itemId=item_id,
        durationWatched=duration_watched
    )
    
    if background_tasks:
        background_tasks.add_task(
            process_events_async,
            current_user["uid"],
            [event]
        )
    
    return {"success": True, "itemId": item_id}
