"""
Firestore Service

Handles all Firestore operations for user data, preferences, and analytics.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from ..config import get_settings
from ..core.logging import get_logger
from ..core.security import initialize_firebase
from ..models.user import UserContext, UserPreferences

logger = get_logger(__name__)
settings = get_settings()

# Firestore client singleton
_db = None


def get_firestore_client():
    """Get or initialize Firestore client."""
    global _db
    if _db is None:
        initialize_firebase()
        _db = firestore.client()
    return _db


class FirestoreService:
    """
    Firestore operations for feed backend.
    
    Collections used:
    - users/{uid}: User profile and preferences
    - users/{uid}/favorites: User's favorite movies/shows
    - users/{uid}/watchlist: User's watchlist
    - users/{uid}/watching: Currently watching
    - users/{uid}/finished: Completed titles
    - users/{uid}/user_titles: Ratings and reactions
    - users/{uid}/following: Friend list
    - activity_logs: User activity for friend feeds
    - analytics_events: Batched analytics
    """
    
    def __init__(self):
        self.db = get_firestore_client()
    
    # =========================================================================
    # USER PREFERENCES
    # =========================================================================
    
    async def get_user_preferences(self, user_id: str) -> UserPreferences:
        """
        Load user preferences from Firestore.
        
        Path: users/{uid} -> preferences field
        """
        try:
            doc = self.db.collection("users").document(user_id).get()
            
            if not doc.exists:
                logger.debug("user_not_found", uid=user_id)
                return UserPreferences()
            
            data = doc.to_dict()
            prefs = data.get("preferences", {})
            
            return UserPreferences(
                selectedGenres=prefs.get("selectedGenres", []),
                selectedGenreIds=prefs.get("selectedGenreIds", []),
                streamingProviders=[
                    p.get("providerName", "") 
                    for p in prefs.get("streamingProviders", [])
                ]
            )
            
        except Exception as e:
            logger.error("get_preferences_failed", uid=user_id, error=str(e))
            return UserPreferences()
    
    # =========================================================================
    # FRIEND LIST
    # =========================================================================
    
    async def get_friend_ids(self, user_id: str) -> List[str]:
        """
        Get list of friend UIDs.
        
        Path: users/{uid}/following
        """
        try:
            docs = self.db.collection("users").document(user_id).collection("following").stream()
            return [doc.id for doc in docs]
        except Exception as e:
            logger.error("get_friends_failed", uid=user_id, error=str(e))
            return []
    
    # =========================================================================
    # USER LISTS (for personalization seeds)
    # =========================================================================
    
    async def get_user_favorites(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get user's favorite titles."""
        return await self._get_user_collection(user_id, "favorites", limit)
    
    async def get_user_watchlist(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get user's watchlist."""
        return await self._get_user_collection(user_id, "watchlist", limit)
    
    async def get_user_watching(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get titles user is currently watching."""
        return await self._get_user_collection(user_id, "watching", limit)
    
    async def get_user_finished(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get titles user has finished."""
        return await self._get_user_collection(user_id, "finished", limit)
    
    async def _get_user_collection(
        self, 
        user_id: str, 
        collection_name: str, 
        limit: int = 10
    ) -> List[Dict]:
        """Generic helper to fetch user subcollection."""
        try:
            docs = (
                self.db.collection("users")
                .document(user_id)
                .collection(collection_name)
                .limit(limit)
                .stream()
            )
            
            items = []
            for doc in docs:
                data = doc.to_dict()
                data["id"] = doc.id
                items.append(data)
            
            return items
            
        except Exception as e:
            logger.error(
                "get_collection_failed", 
                uid=user_id, 
                collection=collection_name,
                error=str(e)
            )
            return []
    
    # =========================================================================
    # SEEN HISTORY
    # =========================================================================
    
    async def get_seen_item_ids(self, user_id: str, limit: int = 500) -> Set[str]:
        """
        Get recently seen item IDs for deduplication.
        
        Path: users/{uid}/seen_items (ordered by timestamp, limited)
        """
        try:
            docs = (
                self.db.collection("users")
                .document(user_id)
                .collection("seen_items")
                .order_by("timestamp", direction=firestore.Query.DESCENDING)
                .limit(limit)
                .stream()
            )
            
            return {doc.id for doc in docs}
            
        except Exception as e:
            logger.warning("get_seen_items_failed", uid=user_id, error=str(e))
            return set()
    
    async def mark_items_seen(self, user_id: str, item_ids: List[str]):
        """
        Mark items as seen by user.
        
        Uses batched writes for efficiency.
        """
        if not item_ids:
            return
        
        try:
            batch = self.db.batch()
            collection_ref = (
                self.db.collection("users")
                .document(user_id)
                .collection("seen_items")
            )
            
            timestamp = datetime.now(timezone.utc)
            
            for item_id in item_ids:
                doc_ref = collection_ref.document(item_id)
                batch.set(doc_ref, {
                    "timestamp": timestamp,
                    "itemId": item_id
                }, merge=True)
            
            batch.commit()
            logger.debug("marked_items_seen", uid=user_id, count=len(item_ids))
            
        except Exception as e:
            logger.error("mark_seen_failed", uid=user_id, error=str(e))
    
    # =========================================================================
    # FRIEND ACTIVITY
    # =========================================================================
    
    async def get_friend_activity(
        self, 
        friend_ids: List[str], 
        limit: int = 20
    ) -> List[Dict]:
        """
        Get recent activity from friends.
        
        Path: activity_logs (filtered by userId in friend_ids)
        
        Returns items that friends have interacted with (liked, watched, etc.)
        """
        if not friend_ids:
            return []
        
        try:
            # Query activity logs for friends
            # Note: Firestore "in" queries are limited to 30 items
            chunk_size = 30
            all_activity = []
            
            for i in range(0, len(friend_ids), chunk_size):
                chunk = friend_ids[i:i + chunk_size]
                
                docs = (
                    self.db.collection("activity_logs")
                    .where(filter=FieldFilter("userId", "in", chunk))
                    .order_by("timestamp", direction=firestore.Query.DESCENDING)
                    .limit(limit)
                    .stream()
                )
                
                for doc in docs:
                    data = doc.to_dict()
                    # Extract item info for feed
                    if data.get("itemId"):
                        all_activity.append({
                            "id": data.get("itemId"),
                            "tmdbId": data.get("tmdbId"),
                            "mediaType": data.get("mediaType", "movie"),
                            "title": data.get("title", ""),
                            "friendId": data.get("userId"),
                            "action": data.get("action", "watched"),
                            "timestamp": data.get("timestamp"),
                        })
            
            return all_activity[:limit]
            
        except Exception as e:
            logger.error("get_friend_activity_failed", error=str(e))
            return []
    
    # =========================================================================
    # ANALYTICS PERSISTENCE
    # =========================================================================
    
    async def save_analytics_events(
        self, 
        user_id: str, 
        events: List[Dict]
    ):
        """
        Save batched analytics events.
        
        Also updates seen_items for VIEW events.
        """
        if not events:
            return
        
        try:
            batch = self.db.batch()
            
            # Save to analytics_events collection
            events_ref = self.db.collection("analytics_events")
            seen_ids = []
            
            for event in events:
                # Add to analytics collection
                doc_ref = events_ref.document()
                batch.set(doc_ref, {
                    "userId": user_id,
                    "eventType": event.get("eventType"),
                    "itemId": event.get("itemId"),
                    "timestamp": event.get("timestamp", datetime.now(timezone.utc)),
                    "durationWatched": event.get("durationWatched"),
                    "metadata": event.get("metadata"),
                })
                
                # Track VIEW events for deduplication
                if event.get("eventType") == "view":
                    seen_ids.append(event.get("itemId"))
            
            batch.commit()
            
            # Update seen items
            if seen_ids:
                await self.mark_items_seen(user_id, seen_ids)
            
            logger.info(
                "analytics_saved",
                uid=user_id,
                events=len(events),
                seen_updated=len(seen_ids)
            )
            
        except Exception as e:
            logger.error("save_analytics_failed", uid=user_id, error=str(e))
    
    # =========================================================================
    # FULL USER CONTEXT (with Caching)
    # =========================================================================
    
    async def load_user_context(self, user_id: str) -> UserContext:
        """
        Load complete user context for feed generation.
        
        Uses Redis cache to reduce Firestore reads by ~80%.
        Cache TTL: 5 minutes.
        
        Flow:
        1. Check Redis cache for user_context:{uid}
        2. If cached → return immediately (cache hit)
        3. If not → query Firestore in parallel → cache result
        """
        import asyncio
        from .cache_service import get_cache_service
        
        cache = get_cache_service()
        
        # Step 1: Check cache first
        cached_data = await cache.get_user_context(user_id)
        if cached_data:
            logger.info("user_context_cache_hit", uid=user_id)
            return UserContext(
                uid=user_id,
                preferences=UserPreferences(**cached_data.get("preferences", {})),
                friendIds=cached_data.get("friendIds", []),
                seenIds=cached_data.get("seenIds", []),
                favorites=cached_data.get("favorites", []),
                watchlist=cached_data.get("watchlist", []),
            )
        
        # Step 2: Cache miss - query Firestore in parallel
        logger.info("user_context_cache_miss", uid=user_id)
        
        prefs_task = self.get_user_preferences(user_id)
        friends_task = self.get_friend_ids(user_id)
        seen_task = self.get_seen_item_ids(user_id, limit=100)  # Reduced from 500 for cost
        favorites_task = self.get_user_favorites(user_id, limit=5)
        watchlist_task = self.get_user_watchlist(user_id, limit=5)
        
        prefs, friends, seen, favorites, watchlist = await asyncio.gather(
            prefs_task, friends_task, seen_task, favorites_task, watchlist_task
        )
        
        # Build context
        context = UserContext(
            uid=user_id,
            preferences=prefs,
            friendIds=friends,
            seenIds=list(seen),
            favorites=[f.get("id") or f.get("tmdbId") for f in favorites if f],
            watchlist=[w.get("id") or w.get("tmdbId") for w in watchlist if w],
        )
        
        # Step 3: Cache the result
        cache_data = {
            "preferences": {
                "selectedGenres": prefs.selected_genres,
                "selectedGenreIds": prefs.selected_genre_ids,
                "streamingProviders": prefs.streaming_providers,
            },
            "friendIds": friends,
            "seenIds": list(seen),
            "favorites": context.favorites,
            "watchlist": context.watchlist,
        }
        await cache.set_user_context(user_id, cache_data)
        
        return context
    
    async def invalidate_user_cache(self, user_id: str):
        """
        Invalidate user's cached context.
        
        Call this after:
        - User updates preferences
        - User follows/unfollows
        - User adds to favorites/watchlist
        """
        from .cache_service import get_cache_service
        cache = get_cache_service()
        await cache.invalidate_user_context(user_id)
        logger.info("user_cache_invalidated", uid=user_id)


# Singleton instance
_firestore_service: Optional[FirestoreService] = None


def get_firestore_service() -> FirestoreService:
    """Get singleton FirestoreService instance."""
    global _firestore_service
    if _firestore_service is None:
        _firestore_service = FirestoreService()
    return _firestore_service
