"""
API Response Models

Standardized response structures for feed endpoints.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class FeedType(str, Enum):
    """Feed type categories."""
    TRENDING = "trending"
    FOR_YOU = "for_you"
    FOLLOWING = "following"


class FeedMeta(BaseModel):
    """Metadata about the feed response."""
    feed_type: FeedType = Field(alias="feedType")
    page: int = 1
    limit: int = 10
    item_count: int = Field(alias="itemCount")
    has_more: bool = Field(alias="hasMore")
    generated_at: datetime = Field(
        default_factory=datetime.utcnow, 
        alias="generatedAt"
    )
    latency_ms: int = Field(default=0, alias="latencyMs")
    cursor: Optional[str] = Field(None, description="Cursor for next page")
    
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)


class FeedResponse(BaseModel):
    """Complete feed API response."""
    feed: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of hydrated feed items"
    )
    meta: FeedMeta
    
    model_config = ConfigDict(populate_by_name=True)


class EventType(str, Enum):
    """Analytics event types."""
    VIEW = "view"
    LIKE = "like"
    SHARE = "share"
    SKIP = "skip"
    COMPLETE = "complete"  # Watched full video
    SAVE = "save"


class AnalyticsEvent(BaseModel):
    """Single analytics event from client."""
    event_type: EventType = Field(alias="eventType")
    item_id: str = Field(alias="itemId")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration_watched: Optional[int] = Field(
        None, 
        alias="durationWatched",
        description="Seconds watched (for VIEW events)"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional event context"
    )
    
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)


class AnalyticsBatch(BaseModel):
    """Batch of analytics events (sent every 30s from client)."""
    events: List[AnalyticsEvent]
    session_id: Optional[str] = Field(None, alias="sessionId")
    
    model_config = ConfigDict(populate_by_name=True)


class ErrorResponse(BaseModel):
    """Standard error response."""
    error: bool = True
    message: str
    status_code: int = Field(alias="statusCode")
    
    model_config = ConfigDict(populate_by_name=True)
