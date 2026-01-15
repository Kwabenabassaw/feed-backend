"""
YouTube API Service

Fetches YouTube Shorts from specified movie recap/summary channels
using the YouTube Data API v3.
"""

import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import httpx

from ..config import get_settings
from ..core.logging import get_logger
from ..services.quota_manager import QuotaManager

logger = get_logger(__name__)
settings = get_settings()


# YouTube Shorts channels to monitor (movie recaps & summaries)
SHORTS_CHANNELS = {
    "UCMSODLut25cw-W7vOwrl3_A": "Minute Movie Shorts",
    "UC-jA_AOUPDWarx8b8m-bJMw": "Minute Movie Recap Shorts",
    "UChO-Dxyo0J7dlTvVW1qEOmQ": "Foundflix Shorts",
    "UCm1uU_zOgbD1jNEWZxzmqFA": "High Boi Shorts",
    "UCoGGFI_OLcLYp9yhMuy-MJw": "OnLooker",
    "UCkwyrizQehf2nCFKhbBtIng": "Mystery Short Recapped",
    "UCe48Fl2G6dB6HHYA982fkRA": "Heavy Spoilers Clips",
    "UCORuzLhgpQFokcgymTz2ZOA": "Sweet Popcorn Recap",
    "UCXraJsjAyrHSSPjj2AHJ6MA": "Snap Recap",
    "UCZYVfr8MOH14JbX_snkUX4w": "Cinema Summary",
}

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


class YouTubeAPIService:
    """
    YouTube Data API v3 client for fetching Shorts.
    
    Focuses on movie recap/summary Shorts channels.
    Manages API quota carefully (10,000 units/day limit).
    """
    
    def __init__(self, redis_client=None):
        self.api_key = settings.youtube_api_key
        self.quota_manager = QuotaManager(redis_client)
        
        if not self.api_key:
            logger.warning("youtube_api_key_not_set")
    
    async def get_channel_uploads_playlist_id(
        self, channel_id: str, client: httpx.AsyncClient
    ) -> Optional[str]:
        """
        Get the uploads playlist ID for a channel.
        
        Each YouTube channel has an uploads playlist where all videos are stored.
        The playlist ID is derived from the channel ID by replacing 'UC' with 'UU'.
        
        Returns:
            Uploads playlist ID or None if failed
        """
        # YouTube uploads playlist ID is channel ID with UC -> UU
        if channel_id.startswith("UC"):
            return "UU" + channel_id[2:]
        
        # Fallback: fetch from API if ID format is different
        try:
            response = await client.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={
                    "key": self.api_key,
                    "id": channel_id,
                    "part": "contentDetails",
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if items:
                    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
                    
        except Exception as e:
            logger.warning("get_uploads_playlist_failed", channel_id=channel_id, error=str(e))
        
        return None
    
    async def fetch_channel_shorts(
        self, 
        channel_id: str, 
        max_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent Shorts from a YouTube channel.
        
        Uses the playlistItems API to get recent uploads, then filters for Shorts
        (videos under 60 seconds).
        
        Args:
            channel_id: YouTube channel ID
            max_results: Maximum number of Shorts to return
            
        Returns:
            List of Short video dicts with metadata
        """
        if not self.api_key:
            logger.warning("youtube_api_key_not_set")
            return []
        
        channel_name = SHORTS_CHANNELS.get(channel_id, "Unknown")
        
        # Check quota before making requests (1 unit per request)
        if not await self.quota_manager.can_make_request("youtube", cost=2):
            logger.warning("youtube_quota_low_skipping", channel=channel_name)
            return []
        
        shorts = []
        
        async with httpx.AsyncClient() as client:
            # Get uploads playlist ID
            playlist_id = await self.get_channel_uploads_playlist_id(channel_id, client)
            
            if not playlist_id:
                logger.warning("playlist_id_not_found", channel_id=channel_id)
                return []
            
            try:
                # Fetch recent uploads from playlist
                response = await client.get(
                    f"{YOUTUBE_API_BASE}/playlistItems",
                    params={
                        "key": self.api_key,
                        "playlistId": playlist_id,
                        "part": "snippet,contentDetails",
                        "maxResults": max_results * 2,  # Fetch more to filter Shorts
                    },
                    timeout=10.0
                )
                
                await self.quota_manager.record_usage("youtube", cost=1)
                
                if response.status_code != 200:
                    logger.warning("playlist_items_failed", 
                                 status=response.status_code,
                                 channel=channel_name)
                    return []
                
                data = response.json()
                items = data.get("items", [])
                
                # Extract video IDs for duration check
                video_ids = [
                    item["contentDetails"]["videoId"] 
                    for item in items 
                    if item.get("contentDetails", {}).get("videoId")
                ]
                
                if not video_ids:
                    return []
                
                # Get video details to check duration (filter for Shorts)
                videos_response = await client.get(
                    f"{YOUTUBE_API_BASE}/videos",
                    params={
                        "key": self.api_key,
                        "id": ",".join(video_ids),
                        "part": "contentDetails,statistics,snippet",
                    },
                    timeout=10.0
                )
                
                await self.quota_manager.record_usage("youtube", cost=1)
                
                if videos_response.status_code != 200:
                    return []
                
                videos_data = videos_response.json()
                
                for video in videos_data.get("items", []):
                    # Parse duration (ISO 8601 format: PT1M30S)
                    duration_str = video.get("contentDetails", {}).get("duration", "")
                    duration_seconds = self._parse_duration(duration_str)
                    
                    # Shorts are 60 seconds or less
                    if duration_seconds <= 60:
                        snippet = video.get("snippet", {})
                        stats = video.get("statistics", {})
                        
                        shorts.append({
                            "id": video["id"],
                            "youtubeKey": video["id"],
                            "title": snippet.get("title", ""),
                            "description": snippet.get("description", "")[:500],
                            "channelId": channel_id,
                            "channelTitle": snippet.get("channelTitle", channel_name),
                            "publishedAt": snippet.get("publishedAt"),
                            "thumbnail": self._get_best_thumbnail(snippet.get("thumbnails", {})),
                            "duration": duration_str,
                            "durationSeconds": duration_seconds,
                            "viewCount": int(stats.get("viewCount", 0)),
                            "likeCount": int(stats.get("likeCount", 0)),
                            "contentType": "short",
                            "videoType": "short",
                            "source": "youtube_shorts",
                        })
                        
                        if len(shorts) >= max_results:
                            break
                
                logger.info("channel_shorts_fetched", 
                           channel=channel_name, 
                           count=len(shorts))
                
            except Exception as e:
                logger.error("fetch_channel_shorts_error", 
                            channel=channel_name, 
                            error=str(e))
        
        return shorts
    
    async def fetch_all_shorts(self, max_per_channel: int = 5) -> List[Dict[str, Any]]:
        """
        Fetch Shorts from all configured movie recap channels.
        
        Args:
            max_per_channel: Maximum Shorts to fetch per channel
            
        Returns:
            Combined list of all Shorts
        """
        if not self.api_key:
            logger.warning("youtube_api_key_not_set_skipping_shorts")
            return []
        
        logger.info("fetching_all_shorts", num_channels=len(SHORTS_CHANNELS))
        
        all_shorts = []
        
        for channel_id, channel_name in SHORTS_CHANNELS.items():
            # Check quota before each channel
            remaining = await self.quota_manager.get_remaining("youtube")
            if remaining < 100:
                logger.warning("youtube_quota_low_stopping", remaining=remaining)
                break
            
            try:
                shorts = await self.fetch_channel_shorts(channel_id, max_per_channel)
                all_shorts.extend(shorts)
                
                # Small delay between channels to avoid rate limiting
                await asyncio.sleep(0.25)
                
            except Exception as e:
                logger.warning("channel_shorts_fetch_failed", 
                             channel=channel_name, 
                             error=str(e))
        
        logger.info("all_shorts_fetched", total=len(all_shorts))
        return all_shorts
    
    def _parse_duration(self, duration_str: str) -> int:
        """
        Parse ISO 8601 duration to seconds.
        
        Examples:
            PT1M30S -> 90
            PT45S -> 45
            PT2M -> 120
        """
        if not duration_str or not duration_str.startswith("PT"):
            return 0
        
        duration_str = duration_str[2:]  # Remove "PT"
        
        total_seconds = 0
        
        # Parse hours
        if "H" in duration_str:
            hours, duration_str = duration_str.split("H")
            total_seconds += int(hours) * 3600
        
        # Parse minutes
        if "M" in duration_str:
            minutes, duration_str = duration_str.split("M")
            total_seconds += int(minutes) * 60
        
        # Parse seconds
        if "S" in duration_str:
            seconds = duration_str.replace("S", "")
            if seconds:
                total_seconds += int(seconds)
        
        return total_seconds
    
    def _get_best_thumbnail(self, thumbnails: Dict[str, Any]) -> str:
        """Get the best quality thumbnail URL."""
        # Priority: maxres > standard > high > medium > default
        for quality in ["maxres", "standard", "high", "medium", "default"]:
            if quality in thumbnails and thumbnails[quality].get("url"):
                return thumbnails[quality]["url"]
        
        return ""


# Singleton instance
_youtube_service: Optional[YouTubeAPIService] = None


def get_youtube_service(redis_client=None) -> YouTubeAPIService:
    """Get singleton YouTube API service instance."""
    global _youtube_service
    if _youtube_service is None:
        _youtube_service = YouTubeAPIService(redis_client)
    return _youtube_service
