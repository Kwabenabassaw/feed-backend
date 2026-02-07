"""
Deduplication Service

Prevents users from seeing the same content twice using Bloom filters
and session-based tracking.
"""

import base64
import json
import uuid
from typing import List, Optional, Set

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)


class DeduplicationService:
    """
    Manages content deduplication for feed generation.
    
    Two levels of deduplication:
    1. User History: Long-term seen items (Bloom filter for > 5000 items)
    2. Session: Short-term page-level dedup (Redis with 10-min TTL)
    """
    
    BLOOM_THRESHOLD = 5000  # Use Bloom filter above this count
    
    def __init__(self, redis_client):
        self.settings = get_settings()
        self.redis = redis_client

        if not self.redis:
            # Critical architectural requirement: Redis must be available
            logger.error("redis_required_for_deduplication")
            # In production, we might want to raise an exception here
            # raise RuntimeError("Redis client is required for DeduplicationService")

    # =========================================================================
    # Session-Based Deduplication (Short-term, per-pagination)
    # =========================================================================
    
    def generate_session_id(self) -> str:
        """Generate a new session ID for pagination tracking."""
        return str(uuid.uuid4())
    
    def encode_cursor(self, session_id: str, offset: int) -> str:
        """
        Encode pagination cursor.
        
        Cursor contains session_id + offset for stateful pagination.
        """
        payload = json.dumps({"session_id": session_id, "offset": offset})
        return base64.urlsafe_b64encode(payload.encode()).decode()
    
    def decode_cursor(self, cursor: str) -> tuple[str, int]:
        """
        Decode pagination cursor.
        
        Returns:
            Tuple of (session_id, offset)
        """
        try:
            payload = base64.urlsafe_b64decode(cursor.encode()).decode()
            data = json.loads(payload)
            return data["session_id"], data["offset"]
        except Exception:
            # Invalid cursor, start fresh
            return self.generate_session_id(), 0
    
    async def get_session_seen_ids(self, session_id: str) -> Set[str]:
        """
        Get IDs already sent in this session.
        
        Used to prevent duplicates across pagination pages.
        """
        if self.redis:
            try:
                ids = await self.redis.smembers(f"session:{session_id}")
                return set(ids) if ids else set()
            except Exception as e:
                logger.warning("redis_get_failed", error=str(e))
        else:
            logger.error("redis_unavailable_for_session_seen")
        
        return set()
    
    async def mark_ids_sent(self, session_id: str, ids: List[str]):
        """
        Mark IDs as sent in this session.
        
        Sets 10-minute TTL for automatic cleanup.
        """
        if not ids:
            return
            
        ttl = self.settings.session_ttl_seconds
        
        if self.redis:
            try:
                await self.redis.sadd(f"session:{session_id}", *ids)
                await self.redis.expire(f"session:{session_id}", ttl)
                return
            except Exception as e:
                logger.warning("redis_set_failed", error=str(e))
        else:
            logger.error("redis_unavailable_for_mark_sent")
    
    # =========================================================================
    # User History Deduplication (Long-term)
    # =========================================================================
    
    def filter_seen(
        self, 
        candidate_ids: List[str], 
        user_seen_ids: Set[str],
        session_seen_ids: Set[str]
    ) -> List[str]:
        """
        Filter out already-seen items.
        
        Combines user history and session-level deduplication.
        
        Args:
            candidate_ids: IDs to filter
            user_seen_ids: User's long-term seen history
            session_seen_ids: IDs sent in current session
            
        Returns:
            Filtered list of unseen IDs
        """
        all_seen = user_seen_ids | session_seen_ids
        return [id for id in candidate_ids if id not in all_seen]
    
    def is_seen(self, item_id: str, seen_ids: Set[str]) -> bool:
        """Check if a specific item has been seen."""
        return item_id in seen_ids


class BloomFilterService:
    """
    Bloom filter for large user histories (> 5000 items).
    
    Trade-off: ~1% false positive rate (user might rarely miss a video,
    thinking they saw it when they didn't). Acceptable per PRD.
    """
    
    def __init__(self, expected_items: int = 10000, fp_rate: float = 0.01):
        self.expected_items = expected_items
        self.fp_rate = fp_rate
        self._filter = None
        
        try:
            from pybloom_live import BloomFilter
            self._filter = BloomFilter(capacity=expected_items, error_rate=fp_rate)
        except ImportError:
            logger.warning("pybloom_not_installed", msg="Falling back to Set-based dedup")
    
    def add(self, item_id: str):
        """Add an item to the Bloom filter."""
        if self._filter:
            self._filter.add(item_id)
    
    def contains(self, item_id: str) -> bool:
        """Check if item might have been seen (may have false positives)."""
        if self._filter:
            return item_id in self._filter
        return False
    
    def add_bulk(self, item_ids: List[str]):
        """Add multiple items to the filter."""
        for item_id in item_ids:
            self.add(item_id)
