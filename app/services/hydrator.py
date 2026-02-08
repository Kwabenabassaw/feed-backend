"""
Hydrator Service

Enriches item IDs with full metadata from the Content Dictionary.
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx

from ..config import get_settings
from ..core.logging import get_logger
from ..models.feed_item import FeedItem, ContentDictionary

logger = get_logger(__name__)


class Hydrator:
    """
    Enriches selected IDs with full metadata.
    
    Lookups happen against the Content Dictionary stored in Supabase.
    Only fetches metadata for the final selected items (not candidates).
    """
    
    def __init__(self, redis_client=None):
        self.settings = get_settings()
        self.redis = redis_client
        self._content_cache: Dict[str, Dict] = {}
        self._cache_timestamp: float = 0
        self._cache_ttl = 600  # 10 minutes
    
    async def _load_content_dictionary(self) -> Dict[str, Dict]:
        """
        Load the master content dictionary.
        
        Sources (in order of preference):
        1. Redis cache
        2. Supabase storage
        3. Local file (development)
        """
        # Check if cache is valid
        if self._content_cache and (time.time() - self._cache_timestamp) < self._cache_ttl:
            return self._content_cache
        
        # Try Redis cache first
        if self.redis:
            try:
                cached = await self.redis.get("content_dictionary")
                if cached:
                    self._content_cache = json.loads(cached)
                    self._cache_timestamp = time.time()
                    return self._content_cache
            except Exception as e:
                logger.warning("redis_content_fetch_failed", error=str(e))
        
        # Try Supabase
        if self.settings.supabase_url and self.settings.supabase_key:
            try:
                url = f"{self.settings.supabase_url}/storage/v1/object/public/content/master_content.json"
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=30.0)
                    if response.status_code == 200:
                        data = response.json()
                        self._content_cache = {item["id"]: item for item in data}
                        self._cache_timestamp = time.time()
                        return self._content_cache
            except Exception as e:
                logger.warning("supabase_content_fetch_failed", error=str(e))
        
        # Fallback to local file
        local_path = Path("indexes") / "master_content.json"
        if local_path.exists():
            try:
                data = json.loads(local_path.read_text())
                self._content_cache = {item["id"]: item for item in data}
                self._cache_timestamp = time.time()
                return self._content_cache
            except Exception as e:
                logger.warning("local_content_fetch_failed", error=str(e))
        
        logger.warning("no_content_dictionary_found")
        return {}
    
    async def hydrate(
        self, 
        item_ids: List[str],
        source_tags: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch full metadata for selected IDs.
        
        Args:
            item_ids: List of item IDs to hydrate
            source_tags: Optional dict mapping ID to source (trending/genre/friend)
            
        Returns:
            List of fully populated feed items (as dicts for JSON serialization)
        """
        if not item_ids:
            return []
        
        content_dict = await self._load_content_dictionary()
        
        hydrated: List[Dict[str, Any]] = []
        missing_ids: List[str] = []
        
        for item_id in item_ids:
            if item_id in content_dict:
                item_data = content_dict[item_id].copy()  # Copy to avoid mutating cache
                
                # Add source if provided
                if source_tags and item_id in source_tags:
                    item_data["source"] = source_tags[item_id]
                
                # Ensure required fields have defaults (use camelCase for frontend)
                item_data.setdefault("youtubeKey", item_id)
                item_data.setdefault("title", "Unknown Title")
                item_data.setdefault("contentType", "trailer")
                item_data.setdefault("videoType", item_data.get("contentType", "trailer"))
                item_data.setdefault("genres", [])
                item_data.setdefault("source", "trending")
                
                # Validate by converting through model (ensures consistent field names)
                try:
                    from ..models.feed_item import FeedItem
                    validated = FeedItem(**item_data)
                    hydrated.append(validated.model_dump(by_alias=True))
                except Exception as e:
                    logger.warning("hydration_validation_failed", item_id=item_id, error=str(e))
                    # Fall back to raw dict if validation fails
                    hydrated.append(item_data)
            else:
                missing_ids.append(item_id)
        
        if missing_ids:
            logger.warning(
                "hydration_missing_items",
                count=len(missing_ids),
                sample=missing_ids[:5]
            )
            
            # Create minimal entries for missing items
            for item_id in missing_ids:
                # Check if this is an image item (IDs start with "img_")
                is_image = item_id.startswith("img_")
                
                hydrated.append({
                    "id": item_id,
                    "youtubeKey": None if is_image else item_id,
                    "title": "Image" if is_image else "Video",
                    "contentType": "image" if is_image else "trailer",
                    "videoType": "image" if is_image else "trailer",
                    "source": source_tags.get(item_id, "unknown") if source_tags else "unknown",
                    "genres": [],
                })
        
        logger.info(
            "hydration_complete",
            requested=len(item_ids),
            found=len(item_ids) - len(missing_ids),
            missing=len(missing_ids)
        )
        
        return hydrated

    
    async def hydrate_single(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Hydrate a single item by ID."""
        result = await self.hydrate([item_id])
        return result[0] if result else None
    
    def clear_cache(self):
        """Clear the content dictionary cache."""
        self._content_cache.clear()
        self._cache_timestamp = 0
        logger.info("content_cache_cleared")
