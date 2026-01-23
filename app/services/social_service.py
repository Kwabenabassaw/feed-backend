"""
Social Service

Handles interactions with Supabase social graph and activity table.
"""

import os
from datetime import datetime
from typing import List, Optional, Dict, Any
import httpx

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

SUPABASE_URL = settings.supabase_url
SUPABASE_SERVICE_KEY = settings.supabase_service_key

class SocialService:
    """
    Service for Social Graph and Activity Feed.
    Uses Supabase RPC calls for complex queries.
    """
    
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            logger.error("supabase_not_configured_social_service")
            
        self.headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation", # For RPC calls
        }
    
    async def get_activity_feed(
        self, 
        user_id: str, 
        limit: int = 10, 
        cursor: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch activity feed for a user from Supabase.
        
        Args:
            user_id: The ID of the user requesting the feed.
            limit: Number of items to fetch.
            cursor: Timestamp cursor for pagination (ISO string or datetime).
            
        Returns:
            List of activity items (title, user info, activity type).
        """
        if not SUPABASE_URL:
            return []
            
        try:
            # Format cursor if present
            cursor_str = None
            if cursor:
                if isinstance(cursor, datetime):
                    cursor_str = cursor.isoformat()
                else:
                    cursor_str = str(cursor)
            
            payload = {
                "p_user_id": user_id,
                "p_limit": limit,
                "p_cursor": cursor_str
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{SUPABASE_URL}/rest/v1/rpc/get_activity_feed",
                    json=payload,
                    headers=self.headers,
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return data
                else:
                    logger.error(
                        "get_activity_feed_failed", 
                        status=response.status_code, 
                        body=response.text[:200]
                    )
                    return []
                    
        except Exception as e:
            logger.error("get_activity_feed_exception", error=str(e), uid=user_id)
            return []

# Singleton
_social_service: Optional[SocialService] = None

def get_social_service() -> SocialService:
    global _social_service
    if _social_service is None:
        _social_service = SocialService()
    return _social_service
