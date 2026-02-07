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

    async def _get_from_plan(self, session_id: str, offset: int, limit: int) -> List[str]:
        """Fetch IDs from cached feed plan in Redis."""
        if not self.redis:
            return []
        key = f"feed_plan:{session_id}"
        # LRANGE is inclusive
        return await self.redis.lrange(key, offset, offset + limit - 1)

    async def _extend_plan(self, session_id: str, items: List[str]):
        """Append items to the feed plan."""
        if not self.redis or not items:
            return
        key = f"feed_plan:{session_id}"
        await self.redis.rpush(key, *items)
        await self.redis.expire(key, self.settings.session_ttl_seconds)

    async def _generate_batch(
        self,
        user_context: UserContext,
        count: int,
        feed_type: str,
        session_seen: Set[str]
    ) -> List[str]:
        """
        Generate a new batch of candidate items.
        Applies mixing logic, deduplication, and shuffling.
        """
        user_seen = set(user_context.seen_ids)
        selected_ids = []

        if feed_type == "trending":
            # Trending Logic
            buffer_limit = count * 4
            candidates = await self._get_trending_candidates(buffer_limit)
            filtered = self.dedup.filter_seen(candidates, user_seen, session_seen)
            selected_ids = filtered[:count]
        else:
            # Mixed Logic (For You)
            t_count, p_count, f_count = self._calculate_bucket_sizes(count)
            
            # Fetch candidates
            trending_ids = await self._get_trending_candidates(t_count * 2)
            personalized_ids = await self._get_personalized_candidates(user_context, p_count * 2)
            friend_ids = await self._get_friend_candidates(user_context, f_count * 2)
            
            # Filter seen
            trending_filtered = self.dedup.filter_seen(trending_ids, user_seen, session_seen)
            personalized_filtered = self.dedup.filter_seen(personalized_ids, user_seen, session_seen)
            friend_filtered = self.dedup.filter_seen(friend_ids, user_seen, session_seen)
            
            # Collect unique IDs
            collected_set = set()
            collected_ids = []
            
            def add_unique(items, limit_cnt):
                added = 0
                for item in items:
                    if added >= limit_cnt: break
                    if item not in collected_set:
                        collected_ids.append(item)
                        collected_set.add(item)
                        added += 1
            
            add_unique(trending_filtered, t_count)
            add_unique(personalized_filtered, p_count)
            add_unique(friend_filtered, f_count)
            
            # Backfill
            if len(collected_ids) < count:
                remaining = count - len(collected_ids)
                avail = [i for i in trending_filtered if i not in collected_set]
                for i in avail[:remaining]:
                    collected_ids.append(i)
                    collected_set.add(i)
            
            # Shuffle
            selected_ids = self._tiered_shuffle(collected_ids)
            
        # Mix Images
        image_ids = await self.index_pool.get_image_ids(limit=max(10, count // 3))
        final_batch = self._mix_images_into_feed(selected_ids, image_ids)
        
        return final_batch

    async def generate(
        self,
        user_context: UserContext,
        limit: int = 10,
        cursor: Optional[str] = None,
        feed_type: str = "for_you"
    ) -> Tuple[List[str], str]:
        """
        Generate feed item IDs using Redis Feed Plan.
        """
        # 1. Parse Cursor
        if cursor:
            session_id, offset = self.dedup.decode_cursor(cursor)
        else:
            session_id = self.dedup.generate_session_id()
            offset = 0

        # 2. Try to fetch from plan (Fast Path)
        cached_items = await self._get_from_plan(session_id, offset, limit)
        
        if len(cached_items) >= limit:
            # We have enough items in the plan
            next_cursor = self.dedup.encode_cursor(session_id, offset + limit)
            logger.info("feed_plan_hit", uid=user_context.uid, offset=offset)
            return cached_items, next_cursor
        
        # 3. Generate New Batch (Slow Path)
        # We need more items.

        # Get what we've already seen in this session (to avoid dupes in new batch)
        session_seen = await self.dedup.get_session_seen_ids(session_id)

        # Generate ahead (batch size)
        batch_size = max(limit * 3, 50)
        
        logger.info(
            "generating_feed_batch",
            uid=user_context.uid,
            batch_size=batch_size,
            feed_type=feed_type
        )

        new_items = await self._generate_batch(
            user_context,
            batch_size,
            feed_type,
            session_seen
        )
        
        # 4. Update Plan
        if new_items:
            await self._extend_plan(session_id, new_items)
            await self.dedup.mark_ids_sent(session_id, new_items)

        # 5. Fetch Final Slice
        if self.redis:
            # Fetch again to get the contiguous slice including new items
            final_slice = await self._get_from_plan(session_id, offset, limit)
        else:
            # Fallback if Redis is unavailable (e.g. tests)
            # Since we just generated a batch, return the requested slice from it
            final_slice = new_items[:limit]

        next_cursor = self.dedup.encode_cursor(session_id, offset + len(final_slice))

        return final_slice, next_cursor
    
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
