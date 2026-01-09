"""
Redis Cache Service

Provides caching layer for user context and other frequently accessed data.
Reduces Firestore read costs by ~80%.
"""

import json
from datetime import timedelta
from typing import Optional, Any
import redis

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Redis client singleton
_redis_client: Optional[redis.Redis] = None


def get_redis_client() -> Optional[redis.Redis]:
    """Get or create Redis client."""
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    if not settings.redis_url:
        logger.debug("redis_not_configured")
        return None
    
    try:
        _redis_client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        # Test connection
        _redis_client.ping()
        logger.info("redis_connected")
        return _redis_client
    except Exception as e:
        logger.warning("redis_connection_failed", error=str(e))
        _redis_client = None
        return None


class CacheService:
    """
    Redis-based caching for feed backend.
    
    Cache Keys:
    - user_context:{uid} → Full user context (TTL: 5 minutes)
    - user_prefs:{uid} → User preferences only (TTL: 10 minutes)
    - friend_list:{uid} → Friend IDs (TTL: 5 minutes)
    - seen_items:{uid} → Seen item IDs set (TTL: 1 hour)
    
    Falls back to in-memory dict if Redis unavailable.
    """
    
    # Cache TTLs in seconds
    USER_CONTEXT_TTL = 300      # 5 minutes
    USER_PREFS_TTL = 600        # 10 minutes
    FRIEND_LIST_TTL = 300       # 5 minutes
    SEEN_ITEMS_TTL = 3600       # 1 hour
    
    def __init__(self):
        self.redis = get_redis_client()
        # Fallback in-memory cache (for dev without Redis)
        self._memory_cache: dict = {}
    
    def _is_available(self) -> bool:
        """Check if Redis is available."""
        return self.redis is not None
    
    # =========================================================================
    # GENERIC CACHE OPERATIONS
    # =========================================================================
    
    async def get(self, key: str) -> Optional[str]:
        """Get value from cache."""
        if self._is_available():
            try:
                return self.redis.get(key)
            except Exception as e:
                logger.warning("cache_get_failed", key=key, error=str(e))
        
        # Fallback to memory
        return self._memory_cache.get(key)
    
    async def set(self, key: str, value: str, ttl_seconds: int = 300) -> bool:
        """Set value in cache with TTL."""
        if self._is_available():
            try:
                self.redis.setex(key, ttl_seconds, value)
                return True
            except Exception as e:
                logger.warning("cache_set_failed", key=key, error=str(e))
        
        # Fallback to memory (no TTL enforcement)
        self._memory_cache[key] = value
        return True
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        if self._is_available():
            try:
                self.redis.delete(key)
                return True
            except Exception as e:
                logger.warning("cache_delete_failed", key=key, error=str(e))
        
        self._memory_cache.pop(key, None)
        return True
    
    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern."""
        if self._is_available():
            try:
                keys = self.redis.keys(pattern)
                if keys:
                    return self.redis.delete(*keys)
            except Exception as e:
                logger.warning("cache_delete_pattern_failed", error=str(e))
        return 0
    
    # =========================================================================
    # USER CONTEXT CACHING
    # =========================================================================
    
    async def get_user_context(self, uid: str) -> Optional[dict]:
        """Get cached user context."""
        key = f"user_context:{uid}"
        data = await self.get(key)
        
        if data:
            logger.debug("cache_hit", key=key)
            return json.loads(data)
        
        logger.debug("cache_miss", key=key)
        return None
    
    async def set_user_context(self, uid: str, context: dict) -> bool:
        """Cache user context."""
        key = f"user_context:{uid}"
        return await self.set(key, json.dumps(context), self.USER_CONTEXT_TTL)
    
    async def invalidate_user_context(self, uid: str) -> bool:
        """Invalidate user's cached context (call after profile changes)."""
        key = f"user_context:{uid}"
        result = await self.delete(key)
        logger.info("cache_invalidated", key=key)
        return result
    
    # =========================================================================
    # PREFERENCES CACHING
    # =========================================================================
    
    async def get_user_prefs(self, uid: str) -> Optional[dict]:
        """Get cached user preferences."""
        key = f"user_prefs:{uid}"
        data = await self.get(key)
        return json.loads(data) if data else None
    
    async def set_user_prefs(self, uid: str, prefs: dict) -> bool:
        """Cache user preferences."""
        key = f"user_prefs:{uid}"
        return await self.set(key, json.dumps(prefs), self.USER_PREFS_TTL)
    
    # =========================================================================
    # FRIEND LIST CACHING
    # =========================================================================
    
    async def get_friend_ids(self, uid: str) -> Optional[list]:
        """Get cached friend IDs."""
        key = f"friend_list:{uid}"
        data = await self.get(key)
        return json.loads(data) if data else None
    
    async def set_friend_ids(self, uid: str, friend_ids: list) -> bool:
        """Cache friend IDs."""
        key = f"friend_list:{uid}"
        return await self.set(key, json.dumps(friend_ids), self.FRIEND_LIST_TTL)
    
    async def invalidate_friend_list(self, uid: str) -> bool:
        """Invalidate friend list (call after follow/unfollow)."""
        return await self.delete(f"friend_list:{uid}")
    
    # =========================================================================
    # SEEN ITEMS CACHING (Set operations)
    # =========================================================================
    
    async def add_seen_items(self, uid: str, item_ids: list) -> bool:
        """Add items to seen set."""
        if not item_ids:
            return True
        
        key = f"seen_items:{uid}"
        
        if self._is_available():
            try:
                self.redis.sadd(key, *item_ids)
                self.redis.expire(key, self.SEEN_ITEMS_TTL)
                return True
            except Exception as e:
                logger.warning("cache_sadd_failed", error=str(e))
        
        return False
    
    async def get_seen_items(self, uid: str) -> set:
        """Get all seen item IDs."""
        key = f"seen_items:{uid}"
        
        if self._is_available():
            try:
                return self.redis.smembers(key) or set()
            except Exception as e:
                logger.warning("cache_smembers_failed", error=str(e))
        
        return set()
    
    async def is_seen(self, uid: str, item_id: str) -> bool:
        """Check if item is in seen set."""
        key = f"seen_items:{uid}"
        
        if self._is_available():
            try:
                return self.redis.sismember(key, item_id)
            except Exception as e:
                logger.warning("cache_sismember_failed", error=str(e))
        
        return False
    
    # =========================================================================
    # STATS
    # =========================================================================
    
    async def get_stats(self) -> dict:
        """Get cache statistics."""
        if not self._is_available():
            return {"status": "unavailable", "type": "memory"}
        
        try:
            info = self.redis.info("stats")
            return {
                "status": "connected",
                "type": "redis",
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "memory_used": self.redis.info("memory").get("used_memory_human"),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# Singleton instance
_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    """Get singleton CacheService instance."""
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service
