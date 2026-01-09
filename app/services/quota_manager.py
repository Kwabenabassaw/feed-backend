"""
Quota Manager Service

Tracks API usage to prevent hitting YouTube/TMDB rate limits.
"""

from datetime import datetime
from typing import Optional

from ..config import get_settings
from ..core.logging import get_logger
from ..core.exceptions import QuotaExceededError

logger = get_logger(__name__)


class QuotaManager:
    """
    Manages API quota tracking to prevent bans.
    
    YouTube Data API: 10,000 units/day
    - Search: 100 units
    - Video list: 1 unit per video
    - Channel list: 1 unit
    
    We set a safe limit of 9,000 to leave buffer.
    """
    
    APIS = {
        "youtube": {"daily_limit": 9000, "cost_search": 100, "cost_video": 1},
        "tmdb": {"daily_limit": 50000, "cost_request": 1},  # TMDB is more generous
    }
    
    def __init__(self, redis_client=None):
        self.settings = get_settings()
        self.redis = redis_client
        self._local_usage: dict = {}  # Fallback if no Redis
    
    def _get_today_key(self, api_name: str) -> str:
        """Get Redis key for today's usage."""
        today = datetime.now().strftime("%Y-%m-%d")
        return f"quota:{api_name}:{today}"
    
    async def get_usage(self, api_name: str) -> int:
        """Get current usage for an API."""
        key = self._get_today_key(api_name)
        
        if self.redis:
            try:
                usage = await self.redis.get(key)
                return int(usage) if usage else 0
            except Exception as e:
                logger.warning("redis_get_quota_failed", error=str(e))
        
        return self._local_usage.get(key, 0)
    
    async def can_make_request(self, api_name: str, cost: int = 1) -> bool:
        """
        Check if we have quota remaining for a request.
        
        Args:
            api_name: "youtube" or "tmdb"
            cost: Cost of the request in units
            
        Returns:
            True if request can be made, False if quota exceeded
        """
        if api_name not in self.APIS:
            return True  # Unknown API, allow by default
        
        current_usage = await self.get_usage(api_name)
        limit = self.APIS[api_name]["daily_limit"]
        
        return (current_usage + cost) <= limit
    
    async def record_usage(self, api_name: str, cost: int = 1):
        """
        Record API usage.
        
        Args:
            api_name: "youtube" or "tmdb"
            cost: Cost of the request in units
        """
        key = self._get_today_key(api_name)
        
        if self.redis:
            try:
                await self.redis.incrby(key, cost)
                await self.redis.expire(key, 86400)  # 24 hour TTL
                return
            except Exception as e:
                logger.warning("redis_record_quota_failed", error=str(e))
        
        # Fallback to local
        if key not in self._local_usage:
            self._local_usage[key] = 0
        self._local_usage[key] += cost
    
    async def require_quota(self, api_name: str, cost: int = 1):
        """
        Check quota and raise exception if exceeded.
        
        Use this before making API calls.
        """
        if not await self.can_make_request(api_name, cost):
            current = await self.get_usage(api_name)
            limit = self.APIS.get(api_name, {}).get("daily_limit", 0)
            logger.error(
                "quota_exceeded",
                api=api_name,
                current=current,
                limit=limit,
                requested=cost
            )
            raise QuotaExceededError(api_name)
    
    async def get_remaining(self, api_name: str) -> int:
        """Get remaining quota for an API."""
        if api_name not in self.APIS:
            return -1  # Unknown
        
        current = await self.get_usage(api_name)
        limit = self.APIS[api_name]["daily_limit"]
        return max(0, limit - current)
    
    async def get_all_quotas(self) -> dict:
        """Get quota status for all tracked APIs."""
        result = {}
        for api_name in self.APIS:
            current = await self.get_usage(api_name)
            limit = self.APIS[api_name]["daily_limit"]
            result[api_name] = {
                "used": current,
                "limit": limit,
                "remaining": max(0, limit - current),
                "percentage": round((current / limit) * 100, 1)
            }
        return result
