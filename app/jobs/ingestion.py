"""
Content Ingestion Job

Fetches new content from YouTube/TMDB/KinoCheck APIs.
Runs every 30 minutes via APScheduler.

Optimization: Uses YouTube RSS feeds for channel monitoring (free)
instead of the YouTube Data API (costly).
"""

import asyncio
from datetime import datetime
from typing import List, Optional
import httpx
import xml.etree.ElementTree as ET

from ..config import get_settings
from ..core.logging import get_logger
from ..services.quota_manager import QuotaManager

logger = get_logger(__name__)
settings = get_settings()


# YouTube channels to monitor
YOUTUBE_CHANNELS = [
    "UCi8e0iOVk1fEOogdfu4YgfA",  # KinoCheck
    "UCuVFG3nXkDcxEaQXqL3SVgA",  # FilmSelect Trailer
    "UCd8fXR41jfXTcx5w9DqHkAA",  # Movie Trailers Source
]


class IngestionJob:
    """
    Background job for content ingestion.
    
    Sources:
    1. YouTube RSS feeds (free, for detecting new videos)
    2. TMDB API (for trending, new releases)
    3. KinoCheck API (for fresh trailers)
    """
    
    def __init__(self, redis_client=None):
        self.quota_manager = QuotaManager(redis_client)
    
    async def fetch_youtube_rss(self, channel_id: str) -> List[dict]:
        """
        Fetch recent videos from YouTube channel via RSS (FREE!).
        
        No API quota consumed. Returns latest 15 videos.
        """
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10.0)
                if response.status_code != 200:
                    return []
                
                # Parse XML
                root = ET.fromstring(response.text)
                ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
                
                videos = []
                for entry in root.findall("atom:entry", ns):
                    video_id = entry.find("yt:videoId", ns)
                    title = entry.find("atom:title", ns)
                    published = entry.find("atom:published", ns)
                    
                    if video_id is not None and title is not None:
                        videos.append({
                            "id": video_id.text,
                            "youtubeKey": video_id.text,
                            "title": title.text,
                            "publishedAt": published.text if published is not None else None,
                            "source": "youtube_rss",
                            "channelId": channel_id,
                        })
                
                return videos
                
        except Exception as e:
            logger.warning("youtube_rss_failed", channel=channel_id, error=str(e))
            return []
    
    async def fetch_tmdb_trending(self) -> List[dict]:
        """
        Fetch trending content from TMDB.
        
        Consumes 1 quota unit per request.
        """
        if not settings.tmdb_api_key:
            logger.warning("tmdb_api_key_not_set")
            return []
        
        # Check quota before making request
        await self.quota_manager.require_quota("tmdb", cost=2)
        
        videos = []
        
        async with httpx.AsyncClient() as client:
            # Trending movies
            response = await client.get(
                "https://api.themoviedb.org/3/trending/movie/week",
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            await self.quota_manager.record_usage("tmdb", cost=1)
            
            if response.status_code == 200:
                data = response.json()
                for item in data.get("results", [])[:10]:
                    videos.append({
                        "tmdbId": item["id"],
                        "mediaType": "movie",
                        "title": item.get("title", "Unknown"),
                        "overview": item.get("overview"),
                        "posterPath": item.get("poster_path"),
                        "backdropPath": item.get("backdrop_path"),
                        "popularity": item.get("popularity", 0),
                        "releaseDate": item.get("release_date"),
                        "source": "tmdb_trending",
                    })
            
            # Trending TV
            response = await client.get(
                "https://api.themoviedb.org/3/trending/tv/week",
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            await self.quota_manager.record_usage("tmdb", cost=1)
            
            if response.status_code == 200:
                data = response.json()
                for item in data.get("results", [])[:10]:
                    videos.append({
                        "tmdbId": item["id"],
                        "mediaType": "tv",
                        "title": item.get("name", "Unknown"),
                        "overview": item.get("overview"),
                        "posterPath": item.get("poster_path"),
                        "backdropPath": item.get("backdrop_path"),
                        "popularity": item.get("popularity", 0),
                        "releaseDate": item.get("first_air_date"),
                        "source": "tmdb_trending",
                    })
        
        return videos
    
    async def run(self):
        """
        Run the ingestion job.
        
        1. Fetch from all YouTube channels (via RSS - free)
        2. Fetch TMDB trending
        3. Merge and deduplicate
        4. Save to master_content.json
        """
        logger.info("ingestion_job_started")
        start_time = datetime.utcnow()
        
        all_videos = []
        
        # YouTube RSS (free, no quota)
        for channel_id in YOUTUBE_CHANNELS:
            videos = await self.fetch_youtube_rss(channel_id)
            all_videos.extend(videos)
            logger.info("youtube_rss_fetched", channel=channel_id, count=len(videos))
        
        # TMDB trending (uses quota)
        try:
            tmdb_videos = await self.fetch_tmdb_trending()
            all_videos.extend(tmdb_videos)
            logger.info("tmdb_fetched", count=len(tmdb_videos))
        except Exception as e:
            logger.warning("tmdb_fetch_failed", error=str(e))
        
        # TODO: Save to master_content.json / Supabase
        # For now, just log the results
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            "ingestion_job_completed",
            total_items=len(all_videos),
            duration_seconds=duration
        )
        
        return all_videos


async def run_ingestion_job():
    """Entry point for scheduled job."""
    job = IngestionJob()
    await job.run()


if __name__ == "__main__":
    # Manual run for testing
    asyncio.run(run_ingestion_job())
