"""
Feed Generator Service

The "Mixer" that applies the 50/30/20 rule for content selection.
"""

import random
from typing import List, Optional, Set, Tuple

from ..config import get_settings
from ..core.logging import get_logger
from ..models.user import UserContext
from .index_pool import IndexPoolService
from .fallback import FallbackService
from .deduplication import DeduplicationService

logger = get_logger(__name__)


class FeedGenerator:
    """
    Generates personalized feed by mixing content from different sources.
    
    Mixing Ratio (configurable via settings):
    - 50% Trending: Global popular content
    - 30% Personalized: Based on user's genre preferences
    - 20% Friend Activity: Content friends interacted with
    
    Returns only IDs (lightweight) - hydration happens separately.
    """
    
    def __init__(
        self, 
        index_pool: IndexPoolService,
        dedup_service: DeduplicationService,
        redis_client=None
    ):
        self.settings = get_settings()
        self.index_pool = index_pool
        self.dedup = dedup_service
        self.fallback = FallbackService(index_pool)
        self.redis = redis_client
    
    def _calculate_bucket_sizes(self, total: int) -> Tuple[int, int, int]:
        """
        Calculate how many items to fetch from each bucket.
        
        Returns:
            Tuple of (trending_count, personalized_count, friend_count)
        """
        trending = int(total * self.settings.trending_ratio)
        personalized = int(total * self.settings.personalized_ratio)
        friend = total - trending - personalized  # Remainder to friend bucket
        
        return trending, personalized, friend
    
    async def _get_trending_candidates(self, limit: int) -> List[str]:
        """Get trending item IDs."""
        # Fetch more than needed to account for deduplication
        buffer = limit * 3
        ids = await self.index_pool.get_trending_ids(limit=buffer)
        return ids
    
    async def _get_personalized_candidates(
        self, 
        user_context: UserContext, 
        limit: int
    ) -> List[str]:
        """
        Get personalized item IDs based on user preferences.
        
        Uses fallback for cold start users.
        """
        # Fallback handles both cold start and normal cases
        buffer = limit * 3
        ids = await self.fallback.get_personalized_fallback(
            user_context, 
            limit=buffer
        )
        return ids
    
    async def _get_friend_candidates(
        self, 
        user_context: UserContext, 
        limit: int
    ) -> List[str]:
        """
        Get friend activity item IDs from Firestore.
        
        Queries activity_logs for items friends interacted with.
        Falls back to community_hot if user has no friends.
        """
        buffer = limit * 3
        
        # Use fallback for users with no friends
        if self.fallback.is_cold_start_friends(user_context):
            return await self.fallback.get_friend_fallback(user_context, limit=buffer)
        
        # Query Firestore for friend activity
        try:
            from .firestore_service import get_firestore_service
            firestore = get_firestore_service()
            
            friend_activity = await firestore.get_friend_activity(
                friend_ids=user_context.friend_ids,
                limit=buffer
            )
            
            # Extract item IDs from activity
            friend_item_ids = [
                activity.get("id") or activity.get("itemId")
                for activity in friend_activity
                if activity.get("id") or activity.get("itemId")
            ]
            
            if friend_item_ids:
                logger.info(
                    "friend_activity_fetched",
                    uid=user_context.uid,
                    friend_count=len(user_context.friend_ids),
                    activity_count=len(friend_item_ids)
                )
                return friend_item_ids
                
        except Exception as e:
            logger.warning("friend_activity_fetch_failed", error=str(e))
        
        # Fallback to community hot if Firestore query fails
        return await self.index_pool.get_community_hot_ids(limit=buffer)
    
    async def generate(
        self,
        user_context: UserContext,
        limit: int = 10,
        cursor: Optional[str] = None
    ) -> Tuple[List[str], str]:
        """
        Generate feed item IDs.
        
        Args:
            user_context: User's preferences and history
            limit: Number of items to return
            cursor: Pagination cursor (contains session_id)
            
        Returns:
            Tuple of (selected_ids, next_cursor)
        """
        # Parse cursor for session-based deduplication
        if cursor:
            session_id, offset = self.dedup.decode_cursor(cursor)
        else:
            session_id = self.dedup.generate_session_id()
            offset = 0
        
        # Get session-seen IDs (items already sent in this session)
        session_seen = await self.dedup.get_session_seen_ids(session_id)
        
        # Combine with user's long-term seen history
        user_seen = set(user_context.seen_ids)
        
        # Calculate bucket sizes
        t_count, p_count, f_count = self._calculate_bucket_sizes(limit)
        
        logger.info(
            "generating_feed",
            uid=user_context.uid,
            limit=limit,
            trending=t_count,
            personalized=p_count,
            friend=f_count,
            is_cold_start=user_context.is_cold_start
        )
        
        # Fetch candidates from each bucket
        trending_ids = await self._get_trending_candidates(t_count)
        personalized_ids = await self._get_personalized_candidates(user_context, p_count)
        friend_ids = await self._get_friend_candidates(user_context, f_count)
        
        # Filter seen items from each bucket
        trending_filtered = self.dedup.filter_seen(trending_ids, user_seen, session_seen)
        personalized_filtered = self.dedup.filter_seen(personalized_ids, user_seen, session_seen)
        friend_filtered = self.dedup.filter_seen(friend_ids, user_seen, session_seen)
        
        # Collect seen IDs to avoid cross-bucket duplicates
        collected_ids: List[str] = []
        collected_set: Set[str] = set()
        
        # Take from each bucket up to their quota
        for id in trending_filtered[:t_count]:
            if id not in collected_set:
                collected_ids.append(id)
                collected_set.add(id)
        
        for id in personalized_filtered[:p_count]:
            if id not in collected_set:
                collected_ids.append(id)
                collected_set.add(id)
        
        for id in friend_filtered[:f_count]:
            if id not in collected_set:
                collected_ids.append(id)
                collected_set.add(id)
        
        # If we don't have enough, backfill from trending
        if len(collected_ids) < limit:
            remaining = limit - len(collected_ids)
            available = [id for id in trending_filtered if id not in collected_set]
            for id in available[:remaining]:
                collected_ids.append(id)
                collected_set.add(id)
        
        # Light shuffle to add variety (preserve top items)
        selected = self._tiered_shuffle(collected_ids)[:limit]
        
        # Mark these IDs as sent in session
        await self.dedup.mark_ids_sent(session_id, selected)
        
        # Generate next cursor
        next_cursor = self.dedup.encode_cursor(session_id, offset + limit)
        
        logger.info(
            "feed_generated",
            uid=user_context.uid,
            count=len(selected),
            session_id=session_id
        )
        
        return selected, next_cursor
    
    def _tiered_shuffle(self, items: List[str]) -> List[str]:
        """
        Tiered shuffle to preserve ranking intent.
        
        - Top 3 items: No shuffle (preserve exact ranking)
        - Items 4-7: Light shuffle (mild variety)
        - Items 8+: Full shuffle (tail variety)
        """
        if len(items) <= 3:
            return items
        
        top = items[:3]  # No shuffle
        mid = items[3:7]
        tail = items[7:]
        
        # Light shuffle for mid
        if len(mid) > 1:
            random.shuffle(mid)
        
        # Full shuffle for tail
        if len(tail) > 1:
            random.shuffle(tail)
        
        return top + mid + tail
