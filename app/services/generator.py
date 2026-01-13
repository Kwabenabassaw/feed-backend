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
        shuffled_videos = self._tiered_shuffle(collected_ids)[:limit]
        
        # Fetch image IDs and mix into feed (3 videos : 1 image ratio)
        image_ids = await self.index_pool.get_image_ids(limit=50)
        selected = self._mix_images_into_feed(shuffled_videos, image_ids)
        
        # Mark these IDs as sent in session
        await self.dedup.mark_ids_sent(session_id, selected)
        
        # Generate next cursor
        next_cursor = self.dedup.encode_cursor(session_id, offset + limit)
        
        logger.info(
            "feed_generated",
            uid=user_context.uid,
            count=len(selected),
            images=len(image_ids),
            session_id=session_id
        )
        
        return selected, next_cursor
    
    def _tiered_shuffle(self, items: List[str]) -> List[str]:
        """
        Tiered shuffle to add variety while preserving some ranking intent.
        
        - Item 1: Random from top 5 (variety in first video)
        - Items 2-5: Light shuffle
        - Items 6+: Full shuffle (tail variety)
        """
        if len(items) <= 1:
            return items
        
        if len(items) <= 5:
            # For short lists, just shuffle everything
            shuffled = items.copy()
            random.shuffle(shuffled)
            return shuffled
        
        # Pick first video randomly from top 5
        top_5 = items[:5]
        first_video = random.choice(top_5)
        remaining_top = [v for v in top_5 if v != first_video]
        
        # Shuffle the rest of top
        random.shuffle(remaining_top)
        
        # Full shuffle for tail
        tail = items[5:]
        random.shuffle(tail)
        
        return [first_video] + remaining_top + tail
    
    def _mix_images_into_feed(
        self, 
        video_ids: List[str], 
        image_ids: List[str]
    ) -> List[str]:
        """
        Mix image IDs into video feed at 3:1 ratio.
        
        Pattern: video, video, video, IMAGE, video, video, video, IMAGE...
        
        Args:
            video_ids: List of video content IDs
            image_ids: List of image content IDs
            
        Returns:
            Mixed list of IDs with images inserted every 3 videos
        """
        if not image_ids:
            return video_ids
        
        result = []
        img_idx = 0
        
        for i, vid_id in enumerate(video_ids):
            result.append(vid_id)
            
            # Insert an image after every 3 videos
            if (i + 1) % 3 == 0 and img_idx < len(image_ids):
                result.append(image_ids[img_idx])
                img_idx += 1
        
        return result
