"""
Fallback Service

Handles Cold Start scenarios for new users with empty preferences.
"""

from typing import List

from ..core.logging import get_logger
from ..models.user import UserContext
from .index_pool import IndexPoolService

logger = get_logger(__name__)


# Default fallback configuration
DEFAULT_GENRES = ["action", "comedy", "drama"]
COMMUNITY_FALLBACK_LIMIT = 5


class FallbackService:
    """
    Provides fallback content for cold start users.
    
    Detects empty states and auto-fills buckets:
    - Empty genres → Use global trending or default genres
    - Empty friends → Use community hot posts
    """
    
    def __init__(self, index_pool: IndexPoolService):
        self.index_pool = index_pool
    
    def is_cold_start_genres(self, user_context: UserContext) -> bool:
        """Check if user has no genre preferences."""
        return len(user_context.preferences.selected_genres) == 0
    
    def is_cold_start_friends(self, user_context: UserContext) -> bool:
        """Check if user has no friends."""
        return len(user_context.friend_ids) == 0
    
    async def get_personalized_fallback(
        self, 
        user_context: UserContext, 
        limit: int = 3
    ) -> List[str]:
        """
        Get IDs for the personalized (30%) bucket.
        
        If user has no genres:
        - Use default popular genres (action, comedy, drama)
        - Returns mixed IDs from these defaults
        
        Args:
            user_context: User's context
            limit: Number of IDs to return
            
        Returns:
            List of item IDs
        """
        if self.is_cold_start_genres(user_context):
            logger.info(
                "cold_start_genres_fallback",
                uid=user_context.uid,
                using_defaults=DEFAULT_GENRES
            )
            # Use default genres for new users
            return await self.index_pool.get_genre_ids(DEFAULT_GENRES, limit=limit)
        
        # User has preferences, use them
        return await self.index_pool.get_genre_ids(
            user_context.preferences.selected_genres, 
            limit=limit
        )
    
    async def get_friend_fallback(
        self, 
        user_context: UserContext, 
        limit: int = 2
    ) -> List[str]:
        """
        Get IDs for the friend activity (20%) bucket.
        
        If user has no friends:
        - Use community hot posts to simulate social activity
        - Helps new users feel engaged
        
        Args:
            user_context: User's context
            limit: Number of IDs to return
            
        Returns:
            List of item IDs
        """
        if self.is_cold_start_friends(user_context):
            logger.info(
                "cold_start_friends_fallback",
                uid=user_context.uid,
                using="community_hot"
            )
            # Use community hot posts as social substitute
            return await self.index_pool.get_community_hot_ids(limit=limit)
        
        # User has friends - this should be handled by the generator
        # which queries Firestore for friend activity
        return []
    
    def get_fallback_reason(self, is_genre_fallback: bool, is_friend_fallback: bool) -> str:
        """Generate appropriate reason text for fallback content."""
        if is_genre_fallback and is_friend_fallback:
            return "Popular picks to get you started"
        elif is_genre_fallback:
            return "Trending in popular categories"
        elif is_friend_fallback:
            return "Hot in the community"
        return ""
