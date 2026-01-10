"""
KinoCheck Service

Fetches trailers from KinoCheck API (free, no API key required).
Provides YouTube video IDs for trending and latest trailers.
"""

import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import httpx

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


# KinoCheck API constants
KINOCHECK_BASE_URL = "https://api.kinocheck.com"
RATE_LIMIT_DELAY = 0.25  # Seconds between requests


class KinoCheckService:
    """
    KinoCheck API client for fetching trailers.
    
    Provides access to trending and latest trailer streams.
    All trailers include YouTube video IDs.
    """
    
    async def fetch_trending(self, limit: int = 30, page: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch trending trailers from KinoCheck.
        
        Args:
            limit: Maximum trailers to fetch
            page: Page number for pagination
            
        Returns:
            List of trailer dicts with youtube_video_id, tmdb_movie_id, etc.
        """
        logger.info("kinocheck_fetch_trending", limit=limit, page=page)
        
        try:
            async with httpx.AsyncClient() as client:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
                response = await client.get(
                    f"{KINOCHECK_BASE_URL}/trailers/trending",
                    params={"limit": limit, "page": page, "language": "en"},
                    timeout=15.0
                )
                
                if response.status_code != 200:
                    logger.warning("kinocheck_fetch_failed", status=response.status_code)
                    return []
                
                data = response.json()
                trailers = self._parse_response(data)
                
                logger.info("kinocheck_trending_fetched", count=len(trailers))
                return trailers
                
        except Exception as e:
            logger.error("kinocheck_fetch_error", error=str(e))
            return []
    
    async def fetch_latest(self, limit: int = 20, page: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch latest trailers from KinoCheck.
        
        Args:
            limit: Maximum trailers to fetch
            page: Page number for pagination
            
        Returns:
            List of trailer dicts with youtube_video_id, tmdb_movie_id, etc.
        """
        logger.info("kinocheck_fetch_latest", limit=limit, page=page)
        
        try:
            async with httpx.AsyncClient() as client:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
                response = await client.get(
                    f"{KINOCHECK_BASE_URL}/trailers/latest",
                    params={"limit": limit, "page": page, "language": "en"},
                    timeout=15.0
                )
                
                if response.status_code != 200:
                    logger.warning("kinocheck_fetch_failed", status=response.status_code)
                    return []
                
                data = response.json()
                trailers = self._parse_response(data)
                
                logger.info("kinocheck_latest_fetched", count=len(trailers))
                return trailers
                
        except Exception as e:
            logger.error("kinocheck_fetch_error", error=str(e))
            return []
    
    def _parse_response(self, data: Any) -> List[Dict[str, Any]]:
        """
        Parse KinoCheck API response.
        
        KinoCheck returns numbered dict keys: {"0": {...}, "1": {...}, "_metadata": {...}}
        
        Returns:
            List of trailer dicts
        """
        trailers = []
        
        # Handle list response
        if isinstance(data, list):
            for item in data:
                trailer = self._extract_trailer(item)
                if trailer:
                    trailers.append(trailer)
            return trailers
        
        # Handle dict response with numbered keys
        if isinstance(data, dict):
            for key, value in data.items():
                # Skip metadata
                if key.startswith("_"):
                    continue
                # Only process numbered keys
                if key.isdigit() and isinstance(value, dict):
                    trailer = self._extract_trailer(value)
                    if trailer:
                        trailers.append(trailer)
        
        return trailers
    
    def _extract_trailer(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract relevant fields from a KinoCheck trailer item.
        
        Returns:
            Normalized trailer dict or None if missing required fields
        """
        youtube_key = item.get("youtube_video_id")
        
        # Skip if no YouTube key
        if not youtube_key:
            return None
        
        # Extract TMDB ID (movie or show)
        tmdb_movie_id = item.get("tmdb_movie_id")
        tmdb_show_id = item.get("tmdb_show_id")
        tmdb_id = tmdb_movie_id or tmdb_show_id
        media_type = "movie" if tmdb_movie_id else "tv" if tmdb_show_id else None
        
        return {
            "youtubeKey": youtube_key,
            "kinocheck_id": item.get("id"),
            "title": item.get("title", "Unknown"),
            "tmdbId": tmdb_id,
            "mediaType": media_type,
            "imdbId": item.get("imdb_id"),
            "thumbnail": item.get("thumbnail_url"),
            "duration": item.get("duration"),
            "language": item.get("language", "en"),
            "categories": item.get("categories", []),
        }


# Singleton instance
_kinocheck_service: Optional[KinoCheckService] = None


def get_kinocheck_service() -> KinoCheckService:
    """Get singleton KinoCheck service instance."""
    global _kinocheck_service
    if _kinocheck_service is None:
        _kinocheck_service = KinoCheckService()
    return _kinocheck_service
