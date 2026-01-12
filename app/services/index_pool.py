"""
Index Pool Service

Manages loading and caching of genre-based index files from Supabase.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional
import httpx

from ..config import get_settings
from ..core.logging import get_logger
from ..models.feed_item import IndexItem

logger = get_logger(__name__)


class IndexPoolService:
    """
    Manages lightweight index files for fast candidate selection.
    
    Indices are small JSON files containing only IDs, scores, and tags.
    """
    
    # Available genre indices
    GENRES = [
        "action", "comedy", "drama", "horror", "thriller",
        "romance", "scifi", "fantasy", "documentary", "animation"
    ]
    
    def __init__(self):
        self.settings = get_settings()
        self._cache: Dict[str, List[IndexItem]] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_ttl = 300  # 5 minutes
        
    def _get_local_path(self, bucket_name: str) -> Path:
        """Get local path for an index file (development)."""
        return Path("indexes") / f"{bucket_name}.json"
    
    async def _fetch_from_supabase(self, bucket_name: str) -> Optional[List[dict]]:
        """Fetch index from Supabase storage."""
        if not self.settings.supabase_url or not self.settings.supabase_key:
            return None
            
        url = f"{self.settings.supabase_url}/storage/v1/object/public/indexes/{bucket_name}.json"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10.0)
                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.warning("supabase_fetch_failed", bucket=bucket_name, error=str(e))
        
        return None
    
    def _load_from_local(self, bucket_name: str) -> Optional[List[dict]]:
        """Load index from local file (development fallback)."""
        path = self._get_local_path(bucket_name)
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception as e:
                logger.warning("local_load_failed", path=str(path), error=str(e))
        return None
    
    def _is_cache_valid(self, bucket_name: str) -> bool:
        """Check if cached index is still valid."""
        if bucket_name not in self._cache:
            return False
        timestamp = self._cache_timestamps.get(bucket_name, 0)
        return (time.time() - timestamp) < self._cache_ttl
    
    async def load_index(self, bucket_name: str) -> List[IndexItem]:
        """
        Load an index by name with caching.
        
        Args:
            bucket_name: Name of index (e.g., "global_trending", "genre_action")
            
        Returns:
            List of IndexItem objects
        """
        # Check cache first
        if self._is_cache_valid(bucket_name):
            return self._cache[bucket_name]
        
        # Try Supabase first, then local fallback
        data = await self._fetch_from_supabase(bucket_name)
        if data is None:
            data = self._load_from_local(bucket_name)
        
        if data is None:
            logger.warning("index_not_found", bucket=bucket_name)
            return []
        
        # Parse into IndexItem objects
        items = []
        for item in data:
            try:
                items.append(IndexItem.model_validate(item))
            except Exception as e:
                logger.debug("index_item_parse_error", item=item, error=str(e))
        
        # Update cache
        self._cache[bucket_name] = items
        self._cache_timestamps[bucket_name] = time.time()
        
        logger.info("index_loaded", bucket=bucket_name, count=len(items))
        return items
    
    async def get_trending_ids(self, limit: int = 10) -> List[str]:
        """Get top trending item IDs."""
        items = await self.load_index("global_trending")
        # Sort by score descending
        sorted_items = sorted(items, key=lambda x: x.score, reverse=True)
        return [item.id for item in sorted_items[:limit]]
    
    async def get_genre_ids(self, genres: List[str], limit: int = 10) -> List[str]:
        """
        Get item IDs matching specified genres.
        
        Distributes limit across genres evenly.
        """
        if not genres:
            return []
        
        per_genre = max(1, limit // len(genres))
        all_ids: List[str] = []
        seen: set = set()
        
        for genre in genres:
            bucket_name = f"genre_{genre.lower().replace(' ', '_')}"
            items = await self.load_index(bucket_name)
            sorted_items = sorted(items, key=lambda x: x.score, reverse=True)
            
            for item in sorted_items:
                if item.id not in seen:
                    all_ids.append(item.id)
                    seen.add(item.id)
                    if len(all_ids) >= limit:
                        return all_ids
                    if len([i for i in all_ids if i in seen]) >= per_genre:
                        break
        
        return all_ids[:limit]
    
    async def get_community_hot_ids(self, limit: int = 10) -> List[str]:
        """Get hot community post IDs."""
        items = await self.load_index("community_hot")
        sorted_items = sorted(items, key=lambda x: x.score, reverse=True)
        return [item.id for item in sorted_items[:limit]]
    
    def clear_cache(self):
        """Clear all cached indices."""
        self._cache.clear()
        self._cache_timestamps.clear()
        logger.info("index_cache_cleared")
    
    async def get_image_ids(self, limit: int = 10) -> List[str]:
        """
        Get image content IDs (for mixed feeds).
        
        Filters master_content.json for items with contentType == 'image'.
        Checks Supabase first, then local file.
        """
        from pathlib import Path
        import json
        
        data = None
        
        # Try Supabase first (for production/Render)
        if self.settings.supabase_url and self.settings.supabase_key:
            try:
                import httpx
                url = f"{self.settings.supabase_url}/storage/v1/object/public/content/master_content.json"
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=10.0)
                    if response.status_code == 200:
                        data = response.json()
            except Exception as e:
                logger.debug("supabase_image_ids_failed", error=str(e))
        
        # Fall back to local file
        if data is None:
            local_path = Path("indexes") / "master_content.json"
            if local_path.exists():
                try:
                    data = json.loads(local_path.read_text())
                except Exception as e:
                    logger.debug("local_image_ids_failed", error=str(e))
        
        if data is None:
            return []
        
        # Filter for image content
        image_ids = [
            item.get("id") 
            for item in data 
            if item.get("contentType") == "image" and item.get("id")
        ]
        
        # Shuffle to add variety
        import random
        random.shuffle(image_ids)
        
        return image_ids[:limit]
