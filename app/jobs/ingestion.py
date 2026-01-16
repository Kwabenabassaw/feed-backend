"""
Content Ingestion Job

Fetches new content from YouTube/TMDB/KinoCheck APIs.
Runs every 30 minutes via APScheduler.

Optimization: Uses YouTube RSS feeds for channel monitoring (free)
instead of the YouTube Data API (costly).
"""

import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import httpx
import xml.etree.ElementTree as ET

from ..config import get_settings
from ..core.logging import get_logger
from ..services.quota_manager import QuotaManager
from ..services.youtube_api import get_youtube_service
from .kinocheck import get_kinocheck_service

logger = get_logger(__name__)
settings = get_settings()


# TMDB Image Base URLs (matching legacy backend)
TMDB_POSTER_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_BACKDROP_BASE = "https://image.tmdb.org/t/p/original"

# Genre ID to Name mapping (matching legacy backend)
GENRE_ID_TO_NAME = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi", 10770: "TV Movie",
    53: "Thriller", 10752: "War", 37: "Western",
    # TV genres
    10759: "Action & Adventure", 10762: "Kids", 10763: "News",
    10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap",
    10767: "Talk", 10768: "War & Politics",
}

# YouTube channels to monitor
YOUTUBE_CHANNELS = [
    "UCi8e0iOVk1fEOogdfu4YgfA",  # KinoCheck
    "UCuVFG3nXkDcxEaQXqL3SVgA",  # FilmSelect Trailer
    "UCd8fXR41jfXTcx5w9DqHkAA",  # Movie Trailers Source
    "UCWOA1ZGywLbqmigxE4Qlvuw", "UCvC4D8onUfXzvj55ZKMzDDQ", "UCjmJDM5pRKbUlVIzDYYWb6g",
    "UCz97F7dMxBNOfGYu3rx8aCw", "UCq0OueAsdxH6b8nyAspwViw", "UC2-BeLxzUBSs0uSrmzWhJuQ",
    "UCF9imwPMSGz4Vq1NiTWCC7g", "UCJ6nMHaJPZvsJ-HmUmj1SeA", "UCuPivVjnfNo4mb3Oog_frZg",
    "UC1Myj674wRVXB9I4c6Hm5zA", "UCQJWtTnAHhEG5w4uN0udnUQ", "UCE5mQnNl8Q4H2qcv4ikaXeA",
    "UCVTQuK2CaWaTgSsoNkn5AiQ", "UC_976xMxPgzIa290Hqtk-9g", "UCi8e0iOVk1fEOogdfu4YgfA",
    "UC3gNmTGu-TTb7xdiczlZz_g", "UCMawOL0n6QekxpuVanT_KRA", "UCWJ5MfdQZ6jXbF5gYuSAf5Q",
    "UCOL10n-as9dXO2qtjjFUQbQ", "UCOP-gP2WgKUKfFBMnkR3iaA", "UC5hX0jtOEAobccb2dvSnYbw",
    "UCgRQHK8Ttr1j9xCEpCAlgbQ", "UCZ8Sxmkweh65HetaZfR8YuA", "UCIsbLox_y9dCcmptjM_0rhg",
    "UCsEukrAd64fqA7FjwkmZ_Dw", "UC0fTbhgouDMneMzSluKaXAA", "UC2iUwfYi_1FCGGqhOUNx-iA",
    "UCP1iRaFlS5EYjJBryFV9JPw", "UCaWd5_7JhbQBe4dknZhsHJg", "UCVtL1edhT8qqY-j2JIndMzg"
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
    
    async def fetch_tmdb_video_key(self, tmdb_id: int, media_type: str, client: httpx.AsyncClient) -> Optional[str]:
        """
        Fetch YouTube video key (trailer) for a movie/TV show from TMDB.
        
        TMDB API: /movie/{id}/videos or /tv/{id}/videos
        Returns the first YouTube trailer/teaser key, or None if not found.
        """
        videos = await self.fetch_tmdb_all_videos(tmdb_id, media_type, client)
        if videos:
            return videos[0]["key"]
        return None
    
    async def fetch_tmdb_all_videos(
        self, tmdb_id: int, media_type: str, client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL YouTube videos for a movie/TV show from TMDB.
        
        Returns list of videos with types: Trailer, Behind the Scenes, Clip, Featurette, Teaser
        """
        # Video types to fetch (in priority order for sorting)
        VIDEO_TYPES = ["Trailer", "Behind the Scenes", "Clip", "Featurette", "Teaser", "Bloopers"]
        
        try:
            endpoint = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/videos"
            response = await client.get(
                endpoint,
                params={"api_key": settings.tmdb_api_key},
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                # Filter YouTube videos and extract key info
                videos = []
                for video in results:
                    if video.get("site") == "YouTube" and video.get("key"):
                        video_type = video.get("type", "Unknown")
                        if video_type in VIDEO_TYPES:
                            videos.append({
                                "key": video.get("key"),
                                "type": video_type,
                                "name": video.get("name", ""),
                                "official": video.get("official", False),
                            })
                
                # Sort by priority (Trailer first, then BTS, etc.)
                type_priority = {vt: i for i, vt in enumerate(VIDEO_TYPES)}
                videos.sort(key=lambda v: type_priority.get(v["type"], 99))
                
                return videos
                        
        except Exception as e:
            logger.debug("tmdb_video_fetch_failed", tmdb_id=tmdb_id, error=str(e))
        
        return []
    
    async def fetch_tmdb_images(
        self, tmdb_id: int, media_type: str, client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """
        Fetch movie stills and backdrops from TMDB.
        
        TMDB API: /movie/{id}/images or /tv/{id}/images
        Returns list of image URLs with metadata.
        """
        TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
        
        try:
            endpoint = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/images"
            response = await client.get(
                endpoint,
                params={"api_key": settings.tmdb_api_key},
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                images = []
                
                # Get backdrops (landscape images - best for feed)
                for img in data.get("backdrops", [])[:3]:  # Max 3 per movie
                    if img.get("file_path"):
                        images.append({
                            "url": f"{TMDB_IMAGE_BASE}{img['file_path']}",
                            "type": "backdrop",
                            "width": img.get("width", 0),
                            "height": img.get("height", 0),
                            "aspectRatio": img.get("aspect_ratio", 1.78),
                        })
                
                # Get stills (for TV shows - scene captures)
                if media_type == "tv":
                    for img in data.get("stills", [])[:2]:  # Max 2 stills
                        if img.get("file_path"):
                            images.append({
                                "url": f"{TMDB_IMAGE_BASE}{img['file_path']}",
                                "type": "still",
                                "width": img.get("width", 0),
                                "height": img.get("height", 0),
                                "aspectRatio": img.get("aspect_ratio", 1.78),
                            })
                
                return images
                        
        except Exception as e:
            logger.debug("tmdb_images_fetch_failed", tmdb_id=tmdb_id, error=str(e))
        
        return []
    
    async def fetch_tmdb_trending(self) -> List[dict]:
        """
        Fetch trending content from TMDB with YouTube trailer keys.
        
        Fetches trending movies/TV and then gets YouTube video keys for each.
        Normalizes items to match legacy backend format.
        """
        if not settings.tmdb_api_key:
            logger.warning("tmdb_api_key_not_set")
            return []
        
        # Check quota before making requests
        await self.quota_manager.require_quota("tmdb", cost=22)  # 2 for trending + 20 for video lookups
        
        videos = []
        
        async with httpx.AsyncClient() as client:
            # Trending movies
            response = await client.get(
                "https://api.themoviedb.org/3/trending/movie/week",
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            await self.quota_manager.record_usage("tmdb", cost=1)
            
            movie_items = []
            if response.status_code == 200:
                data = response.json()
                movie_items = data.get("results", [])[:10]
            
            # Fetch ALL videos for movies (trailers, BTS, clips, featurettes)
            for item in movie_items:
                tmdb_id = item["id"]
                all_videos = await self.fetch_tmdb_all_videos(tmdb_id, "movie", client)
                
                # Skip items without any YouTube videos
                if not all_videos:
                    logger.debug("tmdb_movie_no_videos", tmdb_id=tmdb_id)
                    continue
                
                # Create a feed item for each video (up to 3 per movie)
                for video_info in all_videos[:3]:
                    normalized = self._normalize_tmdb_item(
                        item, "movie", video_info["key"]
                    )
                    # Set the video type (trailer, bts, clip, etc.)
                    normalized["videoType"] = video_info["type"].lower().replace(" ", "_")
                    normalized["videoName"] = video_info.get("name", "")
                    # Make ID unique by including video key
                    normalized["id"] = f"{video_info['key']}"
                    videos.append(normalized)
                    logger.debug("tmdb_movie_video_added", 
                                 tmdb_id=tmdb_id, 
                                 video_type=video_info["type"],
                                 youtube_key=video_info["key"])
            
            # Trending TV
            response = await client.get(
                "https://api.themoviedb.org/3/trending/tv/week",
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            await self.quota_manager.record_usage("tmdb", cost=1)
            
            tv_items = []
            if response.status_code == 200:
                data = response.json()
                tv_items = data.get("results", [])[:10]
            
            # Fetch ALL videos for TV shows (trailers, BTS, clips, featurettes)
            for item in tv_items:
                tmdb_id = item["id"]
                all_videos = await self.fetch_tmdb_all_videos(tmdb_id, "tv", client)
                
                # Skip items without any YouTube videos
                if not all_videos:
                    logger.debug("tmdb_tv_no_videos", tmdb_id=tmdb_id)
                    continue
                
                # Create a feed item for each video (up to 3 per show)
                for video_info in all_videos[:3]:
                    normalized = self._normalize_tmdb_item(
                        item, "tv", video_info["key"]
                    )
                    # Set the video type (trailer, bts, clip, etc.)
                    normalized["videoType"] = video_info["type"].lower().replace(" ", "_")
                    normalized["videoName"] = video_info.get("name", "")
                    # Make ID unique by including video key
                    normalized["id"] = f"{video_info['key']}"
                    videos.append(normalized)
                    logger.debug("tmdb_tv_video_added", 
                                 tmdb_id=tmdb_id, 
                                 video_type=video_info["type"],
                                 youtube_key=video_info["key"])
        
        # Log summary
        logger.info("tmdb_videos_fetched", total=len(videos))
        
        return videos
    
    def _normalize_tmdb_item(self, item: Dict[str, Any], media_type: str, youtube_key: str) -> Dict[str, Any]:
        """
        Normalize TMDB item to match legacy backend format.
        
        Args:
            item: Raw TMDB item from API response
            media_type: "movie" or "tv"
            youtube_key: YouTube video key
            
        Returns:
            Normalized feed item dict
        """
        tmdb_id = item.get("id")
        
        # Extract title (different field for movies vs TV)
        title = item.get("title") or item.get("name") or "Unknown"
        
        # Build full poster/backdrop URLs
        poster_path = item.get("poster_path")
        backdrop_path = item.get("backdrop_path")
        
        poster = f"{TMDB_POSTER_BASE}{poster_path}" if poster_path else f"https://img.youtube.com/vi/{youtube_key}/hqdefault.jpg"
        backdrop = f"{TMDB_BACKDROP_BASE}{backdrop_path}" if backdrop_path else None
        
        # Convert genre IDs to names
        genre_ids = item.get("genre_ids", [])
        genres = [GENRE_ID_TO_NAME.get(gid) for gid in genre_ids if gid in GENRE_ID_TO_NAME]
        
        # Release date
        release_date = item.get("release_date") or item.get("first_air_date")
        
        now = datetime.now(timezone.utc).isoformat()
        
        return {
            "id": youtube_key,  # Use YouTube key as primary ID
            "youtubeKey": youtube_key,
            "tmdbId": tmdb_id,
            "mediaType": media_type,
            "title": title,
            "overview": item.get("overview", ""),
            "poster": poster,
            "posterPath": poster_path,
            "backdrop": backdrop,
            "backdropPath": backdrop_path,
            "genres": genres,
            "genreIds": genre_ids,
            "popularity": item.get("popularity", 0),
            "voteAverage": item.get("vote_average", 0),
            "releaseDate": release_date,
            "language": item.get("original_language", "en"),
            "videoType": "trailer",
            "source": "tmdb_trending",
            "updatedAt": now,
        }
    
    async def fetch_tmdb_discover_by_genre(self) -> List[dict]:
        """
        Fetch content from TMDB /discover endpoint for each major genre.
        
        This ensures we have content for all genre buckets, not just what's
        trending. Fetches top 5 movies per genre.
        """
        if not settings.tmdb_api_key:
            logger.warning("tmdb_api_key_not_set")
            return []
        
        # Major genres to fetch (genre_id: genre_name)
        GENRES_TO_FETCH = {
            28: "action",
            35: "comedy", 
            18: "drama",
            27: "horror",
            53: "thriller",
            10749: "romance",
            878: "scifi",
            14: "fantasy",
            16: "animation",
            99: "documentary",
        }
        
        videos = []
        seen_tmdb_ids = set()
        
        async with httpx.AsyncClient() as client:
            for genre_id, genre_name in GENRES_TO_FETCH.items():
                try:
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(0.25)
                    
                    # Fetch discover movies for this genre
                    response = await client.get(
                        "https://api.themoviedb.org/3/discover/movie",
                        params={
                            "api_key": settings.tmdb_api_key,
                            "with_genres": genre_id,
                            "sort_by": "popularity.desc",
                            "page": 1,
                        },
                        timeout=10.0
                    )
                    
                    if response.status_code != 200:
                        logger.warning("discover_fetch_failed", genre=genre_name, status=response.status_code)
                        continue
                    
                    data = response.json()
                    items = data.get("results", [])[:5]  # Top 5 per genre
                    
                    genre_count = 0
                    for item in items:
                        tmdb_id = item.get("id")
                        
                        # Skip if already processed
                        if tmdb_id in seen_tmdb_ids:
                            continue
                        seen_tmdb_ids.add(tmdb_id)
                        
                        # Get YouTube trailer
                        youtube_key = await self.fetch_tmdb_video_key(tmdb_id, "movie", client)
                        if not youtube_key:
                            continue
                        
                        normalized = self._normalize_tmdb_item(item, "movie", youtube_key)
                        normalized["source"] = f"discover_{genre_name}"
                        videos.append(normalized)
                        genre_count += 1
                    
                    logger.info("discover_genre_fetched", genre=genre_name, count=genre_count)
                    
                except Exception as e:
                    logger.warning("discover_genre_error", genre=genre_name, error=str(e))
        
        logger.info("discover_total_fetched", total=len(videos))
        return videos
    
    async def fetch_tmdb_released_today(self) -> List[dict]:
        """
        Fetch trailers for movies and TV shows released TODAY.
        
        Uses TMDB /discover endpoint with primary_release_date filtering for movies
        and first_air_date filtering for TV shows.
        
        Returns:
            List of feed items for content released today
        """
        if not settings.tmdb_api_key:
            logger.warning("tmdb_api_key_not_set")
            return []
        
        # Get today's date in YYYY-MM-DD format
        today = datetime.now(timezone.utc).date().isoformat()
        
        logger.info("fetching_released_today", date=today)
        
        videos = []
        seen_tmdb_ids = set()
        
        async with httpx.AsyncClient() as client:
            # Fetch movies released today
            try:
                await asyncio.sleep(0.25)
                
                response = await client.get(
                    "https://api.themoviedb.org/3/discover/movie",
                    params={
                        "api_key": settings.tmdb_api_key,
                        "primary_release_date.gte": today,
                        "primary_release_date.lte": today,
                        "sort_by": "popularity.desc",
                        "page": 1,
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    movie_items = data.get("results", [])[:20]  # Max 20 movies
                    
                    logger.info("released_today_movies_found", count=len(movie_items))
                    
                    # Fetch ALL videos for each movie
                    for item in movie_items:
                        tmdb_id = item.get("id")
                        
                        if tmdb_id in seen_tmdb_ids:
                            continue
                        seen_tmdb_ids.add(tmdb_id)
                        
                        # Get ALL YouTube videos (trailers, clips, BTS, etc.)
                        all_videos = await self.fetch_tmdb_all_videos(tmdb_id, "movie", client)
                        
                        if not all_videos:
                            logger.debug("released_today_movie_no_videos", tmdb_id=tmdb_id)
                            continue
                        
                        # Create a feed item for each video (up to 3 per movie)
                        for video_info in all_videos[:3]:
                            normalized = self._normalize_tmdb_item(
                                item, "movie", video_info["key"]
                            )
                            normalized["videoType"] = video_info["type"].lower().replace(" ", "_")
                            normalized["videoName"] = video_info.get("name", "")
                            normalized["id"] = f"{video_info['key']}"
                            normalized["source"] = "released_today"
                            videos.append(normalized)
                            
                            logger.debug("released_today_movie_added", 
                                       tmdb_id=tmdb_id, 
                                       video_type=video_info["type"])
                else:
                    logger.warning("released_today_movies_fetch_failed", 
                                 status=response.status_code)
                    
            except Exception as e:
                logger.warning("released_today_movies_error", error=str(e))
            
            # Fetch TV shows with first air date today
            try:
                await asyncio.sleep(0.25)
                
                response = await client.get(
                    "https://api.themoviedb.org/3/discover/tv",
                    params={
                        "api_key": settings.tmdb_api_key,
                        "first_air_date.gte": today,
                        "first_air_date.lte": today,
                        "sort_by": "popularity.desc",
                        "page": 1,
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    tv_items = data.get("results", [])[:20]  # Max 20 shows
                    
                    logger.info("released_today_tv_found", count=len(tv_items))
                    
                    # Fetch ALL videos for each TV show
                    for item in tv_items:
                        tmdb_id = item.get("id")
                        
                        if tmdb_id in seen_tmdb_ids:
                            continue
                        seen_tmdb_ids.add(tmdb_id)
                        
                        # Get ALL YouTube videos (trailers, clips, BTS, etc.)
                        all_videos = await self.fetch_tmdb_all_videos(tmdb_id, "tv", client)
                        
                        if not all_videos:
                            logger.debug("released_today_tv_no_videos", tmdb_id=tmdb_id)
                            continue
                        
                        # Create a feed item for each video (up to 3 per show)
                        for video_info in all_videos[:3]:
                            normalized = self._normalize_tmdb_item(
                                item, "tv", video_info["key"]
                            )
                            normalized["videoType"] = video_info["type"].lower().replace(" ", "_")
                            normalized["videoName"] = video_info.get("name", "")
                            normalized["id"] = f"{video_info['key']}"
                            normalized["source"] = "released_today"
                            videos.append(normalized)
                            
                            logger.debug("released_today_tv_added", 
                                       tmdb_id=tmdb_id, 
                                       video_type=video_info["type"])
                else:
                    logger.warning("released_today_tv_fetch_failed", 
                                 status=response.status_code)
                    
            except Exception as e:
                logger.warning("released_today_tv_error", error=str(e))
        
        logger.info("released_today_total_fetched", date=today, total=len(videos))
        return videos
    
    async def fetch_image_feed_items(self) -> List[dict]:
        """
        Fetch image feed items (movie stills/backdrops) from TMDB trending.
        
        Creates image-type content items to be mixed between videos in the feed.
        Fetches 1 image per trending movie (max 15 images).
        """
        if not settings.tmdb_api_key:
            return []
        
        images = []
        
        async with httpx.AsyncClient() as client:
            # Get trending movies for image content
            response = await client.get(
                "https://api.themoviedb.org/3/trending/movie/week",
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            items = data.get("results", [])[:50]  # Max 50 movies
            
            for item in items:
                tmdb_id = item["id"]
                title = item.get("title", "Unknown")
                
                # Fetch images for this movie
                movie_images = await self.fetch_tmdb_images(tmdb_id, "movie", client)
                
                # Take only the first (best) backdrop
                if movie_images:
                    img = movie_images[0]
                    
                    # Build image feed item
                    poster_path = item.get("poster_path")
                    
                    images.append({
                        "id": f"img_{tmdb_id}",
                        "youtubeKey": None,  # No video for images
                        "contentType": "image",  # KEY: marks this as image
                        "imageUrl": img["url"],
                        "imageType": img["type"],
                        "tmdbId": tmdb_id,
                        "mediaType": "movie",
                        "title": title,
                        "overview": item.get("overview", ""),
                        "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
                        "posterPath": poster_path,
                        "voteAverage": item.get("vote_average", 0),
                        "releaseDate": item.get("release_date"),
                        "genres": [],
                        "source": "tmdb_image",
                    })
                    
                    logger.debug("image_feed_item_created", tmdb_id=tmdb_id, title=title)
        
        logger.info("image_feed_items_fetched", count=len(images))
        return images
    
    async def fetch_kinocheck_trailers(self) -> List[dict]:
        """
        Fetch trailers from KinoCheck (trending + latest).
        
        Validates each trailer against TMDB - rejects items without TMDB ID.
        """
        kinocheck = get_kinocheck_service()
        all_trailers = []
        validated = []
        
        # Fetch trending trailers (page 1)
        try:
            trending = await kinocheck.fetch_trending(limit=50, page=1)
            all_trailers.extend(trending)
            logger.info("kinocheck_trending_p1_raw", count=len(trending))
        except Exception as e:
            logger.warning("kinocheck_trending_failed", error=str(e))
        
        # Fetch trending trailers (page 2 for more variety)
        try:
            trending_p2 = await kinocheck.fetch_trending(limit=50, page=2)
            all_trailers.extend(trending_p2)
            logger.info("kinocheck_trending_p2_raw", count=len(trending_p2))
        except Exception as e:
            logger.warning("kinocheck_trending_p2_failed", error=str(e))
        
        # Fetch latest trailers
        try:
            latest = await kinocheck.fetch_latest(limit=50, page=1)
            all_trailers.extend(latest)
            logger.info("kinocheck_latest_raw", count=len(latest))
        except Exception as e:
            logger.warning("kinocheck_latest_failed", error=str(e))
        
        # Fetch by popular genres (10 each from 5 genres = 50 more trailers)
        popular_genres = ["Action", "Horror", "Comedy", "Thriller", "Science Fiction"]
        for genre in popular_genres:
            try:
                genre_trailers = await kinocheck.fetch_by_genre(genre, limit=10)
                all_trailers.extend(genre_trailers)
                logger.info("kinocheck_genre_raw", genre=genre, count=len(genre_trailers))
            except Exception as e:
                logger.warning("kinocheck_genre_failed", genre=genre, error=str(e))
        
        # Validate and normalize each trailer
        async with httpx.AsyncClient() as client:
            for trailer in all_trailers:
                youtube_key = trailer.get("youtubeKey")
                if not youtube_key:
                    continue
                
                tmdb_id = trailer.get("tmdbId")
                media_type = trailer.get("mediaType")
                title = trailer.get("title", "")
                
                # Fallback 1: Look up via IMDB ID
                if not tmdb_id and trailer.get("imdbId"):
                    tmdb_id, media_type = await self._lookup_tmdb_by_imdb(
                        trailer["imdbId"], client
                    )
                
                # Fallback 2: Search by title (extract movie name from trailer title)
                if not tmdb_id and title:
                    tmdb_id, media_type = await self._search_tmdb_by_title(
                        title, client
                    )
                
                # Skip if still no TMDB ID (orphan trailer)
                if not tmdb_id:
                    logger.debug("kinocheck_rejected_no_tmdb", 
                                 title=title)
                    continue
                
                # Fetch full metadata from TMDB
                normalized = await self._enrich_from_tmdb(
                    tmdb_id, media_type, youtube_key, client
                )
                
                if normalized:
                    normalized["source"] = "kinocheck"
                    normalized["kinocheck_id"] = trailer.get("kinocheck_id")
                    validated.append(normalized)
                
                # Rate limit to avoid hammering TMDB
                await asyncio.sleep(0.1)
        
        logger.info("kinocheck_validated", 
                    raw=len(all_trailers), 
                    validated=len(validated))
        return validated
    
    async def _lookup_tmdb_by_imdb(
        self, imdb_id: str, client: httpx.AsyncClient
    ) -> tuple[Optional[int], Optional[str]]:
        """
        Look up TMDB ID using IMDB ID.
        
        Returns:
            Tuple of (tmdb_id, media_type) or (None, None) if not found
        """
        if not settings.tmdb_api_key:
            return None, None
        
        try:
            response = await client.get(
                f"https://api.themoviedb.org/3/find/{imdb_id}",
                params={
                    "api_key": settings.tmdb_api_key,
                    "external_source": "imdb_id"
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Check movie results
                movie_results = data.get("movie_results", [])
                if movie_results:
                    return movie_results[0]["id"], "movie"
                
                # Check TV results
                tv_results = data.get("tv_results", [])
                if tv_results:
                    return tv_results[0]["id"], "tv"
        
        except Exception as e:
            logger.debug("imdb_lookup_failed", imdb_id=imdb_id, error=str(e))
        
        return None, None
    
    async def _search_tmdb_by_title(
        self, trailer_title: str, client: httpx.AsyncClient
    ) -> tuple[Optional[int], Optional[str]]:
        """
        Search TMDB by title extracted from trailer title.
        
        Trailer titles are usually: "MOVIE NAME Official Trailer (2024)"
        We extract the movie name and search TMDB.
        
        Returns:
            Tuple of (tmdb_id, media_type) or (None, None) if not found
        """
        if not settings.tmdb_api_key or not trailer_title:
            return None, None
        
        # Extract movie/show name from trailer title
        # Common patterns: "MOVIE NAME Official Trailer (2024)", "MOVIE NAME Trailer (2024)"
        import re
        
        # Remove common trailer suffixes
        clean_title = trailer_title
        patterns_to_remove = [
            r'\s*Official\s*(New\s*)?(Final\s*)?(Trailer|Teaser|Clip).*$',
            r'\s*Trailer\s*\d*.*$',
            r'\s*Teaser.*$',
            r'\s*\(\d{4}\).*$',
            r'\s*-\s*\d+\s*Minute.*$',
            r'\s*Season\s*\d+.*$',
            r'\s*Chapter\s*\d+.*$',
        ]
        
        for pattern in patterns_to_remove:
            clean_title = re.sub(pattern, '', clean_title, flags=re.IGNORECASE)
        
        clean_title = clean_title.strip()
        
        if not clean_title or len(clean_title) < 2:
            return None, None
        
        try:
            # Search movies first
            response = await client.get(
                "https://api.themoviedb.org/3/search/movie",
                params={
                    "api_key": settings.tmdb_api_key,
                    "query": clean_title,
                    "language": "en-US",
                    "page": 1
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                if results:
                    # Return first result
                    logger.debug("tmdb_title_match", 
                                 query=clean_title, 
                                 matched=results[0].get("title"))
                    return results[0]["id"], "movie"
            
            # If no movie found, try TV search
            response = await client.get(
                "https://api.themoviedb.org/3/search/tv",
                params={
                    "api_key": settings.tmdb_api_key,
                    "query": clean_title,
                    "language": "en-US",
                    "page": 1
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                if results:
                    logger.debug("tmdb_title_match_tv", 
                                 query=clean_title, 
                                 matched=results[0].get("name"))
                    return results[0]["id"], "tv"
        
        except Exception as e:
            logger.debug("title_search_failed", title=clean_title, error=str(e))
        
        return None, None
    
    async def _enrich_from_tmdb(
        self, 
        tmdb_id: int, 
        media_type: str, 
        youtube_key: str,
        client: httpx.AsyncClient
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch full metadata from TMDB for a KinoCheck trailer.
        
        Returns:
            Normalized feed item or None if fetch failed
        """
        if not settings.tmdb_api_key or not media_type:
            return None
        
        try:
            endpoint = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
            response = await client.get(
                endpoint,
                params={"api_key": settings.tmdb_api_key},
                timeout=10.0
            )
            
            if response.status_code != 200:
                return None
            
            item = response.json()
            
            # Use existing normalization
            return self._normalize_tmdb_item(item, media_type, youtube_key)
            
        except Exception as e:
            logger.debug("tmdb_enrich_failed", tmdb_id=tmdb_id, error=str(e))
            return None
    
    async def run(self):
        """
        Run the ingestion job with smart merge and immediate sync.
        
        1. Fetch candidates from all sources (YouTube RSS, TMDB, KinoCheck)
        2. Merge into content dictionary with priority for rich metadata (TMDB/KinoCheck > RSS)
        3. Save to master_content.json locally
        4. Upload immediately to Supabase Storage
        """
        logger.info("ingestion_job_started")
        print("\n" + "="*60)
        print("[Ingestion] 🚀 Starting content ingestion with smart-merge...")
        print("="*60)
        start_time = datetime.utcnow()
        
        # Candidates list (will be merged into a dict)
        candidates = []
        
        # --- PHASE 1: Data Collection ---
        
        # YouTube RSS (free, no quota)
        for channel_id in YOUTUBE_CHANNELS:
            videos = await self.fetch_youtube_rss(channel_id)
            for v in videos:
                v["merge_priority"] = 0  # Lowest priority
            candidates.extend(videos)
        print(f"[Ingestion] ✅ YouTube RSS: {len(candidates)} candidates fetched")
        
        # TMDB trending
        try:
            tmdb_videos = await self.fetch_tmdb_trending()
            for v in tmdb_videos:
                v["merge_priority"] = 2  # High priority (full metadata)
            candidates.extend(tmdb_videos)
            print(f"[Ingestion] ✅ TMDB Trending: {len(tmdb_videos)} candidates fetched")
        except Exception as e:
            logger.warning("tmdb_fetch_failed", error=str(e))
        
        # TMDB discover
        try:
            discover_videos = await self.fetch_tmdb_discover_by_genre()
            for v in discover_videos:
                v["merge_priority"] = 2
            candidates.extend(discover_videos)
            print(f"[Ingestion] ✅ TMDB Discover: {len(discover_videos)} candidates fetched")
        except Exception as e:
            logger.warning("discover_fetch_failed", error=str(e))
            
        # TMDB released today
        try:
            released_today_videos = await self.fetch_tmdb_released_today()
            for v in released_today_videos:
                v["merge_priority"] = 3  # High priority (fresh today!)
            candidates.extend(released_today_videos)
            print(f"[Ingestion] ✅ Released Today: {len(released_today_videos)} candidates fetched")
        except Exception as e:
            logger.warning("released_today_fetch_failed", error=str(e))
            
        # KinoCheck
        try:
            kinocheck_videos = await self.fetch_kinocheck_trailers()
            for v in kinocheck_videos:
                v["merge_priority"] = 3  # Highest priority (KinoCheck specific context)
            candidates.extend(kinocheck_videos)
            print(f"[Ingestion] ✅ KinoCheck: {len(kinocheck_videos)} candidates fetched")
        except Exception as e:
            logger.warning("kinocheck_fetch_failed", error=str(e))
            
        # Images
        image_items = []
        try:
            image_items = await self.fetch_image_feed_items()
            for img in image_items:
                img["merge_priority"] = 1  # Moderate priority
            candidates.extend(image_items)
            print(f"[Ingestion] ✅ Images: {len(image_items)} candidates fetched")
        except Exception as e:
            logger.warning("images_fetch_failed", error=str(e))
            
        # YouTube Shorts (movie recap channels)
        try:
            youtube_service = get_youtube_service()
            shorts = await youtube_service.fetch_all_shorts(max_per_channel=5)
            for short in shorts:
                short["merge_priority"] = 3  # High priority (fresh content)
            candidates.extend(shorts)
            print(f"[Ingestion] ✅ YouTube Shorts: {len(shorts)} candidates fetched")
        except Exception as e:
            logger.warning("youtube_shorts_fetch_failed", error=str(e))

        # --- PHASE 2: Smart Merge ---
        
        from pathlib import Path
        import json
        indexes_dir = Path("indexes")
        indexes_dir.mkdir(exist_ok=True)
        master_path = indexes_dir / "master_content.json"
        
        # Build content map (id -> item)
        content_map = {}
        
        # 1. Load existing content first
        if master_path.exists():
            try:
                existing_content = json.loads(master_path.read_text())
                for item in existing_content:
                    idx = item.get("id") or item.get("youtubeKey")
                    if idx:
                        item.setdefault("merge_priority", 1)  # Assume already enriched
                        content_map[idx] = item
                logger.info("existing_content_loaded", count=len(content_map))
            except Exception as e:
                logger.warning("existing_content_load_failed", error=str(e))

        # 2. Merge new candidates using priority
        new_items_count = 0
        upgraded_count = 0
        
        # Sort candidates by priority so higher priority naturally overwrites if we processed blindly,
        # but we'll use an explicit logic for clarity.
        for candidate in candidates:
            idx = candidate.get("id") or candidate.get("youtubeKey")
            if not idx:
                continue
                
            if idx not in content_map:
                # New item
                content_map[idx] = candidate
                new_items_count += 1
            else:
                # Existing item - check if new one is "better"
                existing = content_map[idx]
                new_priority = candidate.get("merge_priority", 0)
                old_priority = existing.get("merge_priority", 0)
                
                # Upgrade if priority is higher OR if current one is "missing" (Hydrator fallback)
                if new_priority > old_priority or existing.get("isMissing"):
                    # Preserve original source if it was manual/special? (Optional)
                    # For now, just take the richer metadata
                    content_map[idx] = candidate
                    upgraded_count += 1
        
        # --- PHASE 3: Save & Sync ---
        
        # Convert map back to list and sort by updatedAt (or publishedAt)
        # We sort to keep a predictable order (last 5000)
        final_content = list(content_map.values())
        
        # Handle growth limit
        if len(final_content) > 5000:
            # Simple chronological prune or priority prune? 
            # For now, just keep last 5000
            final_content = final_content[-5000:]
            
        # Save locally
        try:
            master_path.write_text(json.dumps(final_content, indent=2, default=str))
            logger.info("master_content_saved_locally", count=len(final_content), new=new_items_count, upgraded=upgraded_count)
            print(f"[Ingestion] ✅ Saved {len(final_content)} items locally ({new_items_count} new, {upgraded_count} upgraded)")
        except Exception as e:
            logger.error("local_save_failed", error=str(e))

        # Upload to Supabase immediately (CRITICAL for Hydrator consistency)
        try:
            from ..services.supabase_storage import get_supabase_storage
            storage = get_supabase_storage()
            content_bytes = json.dumps(final_content, default=str).encode()
            success = await storage.upload_file(
                bucket="content",
                filename="master_content.json",
                content=content_bytes
            )
            if success:
                logger.info("master_content_synced_to_supabase")
                print(f"[Ingestion] ☁️ Successfully synced master_content.json to Supabase")
            else:
                print(f"[Ingestion] ⚠️ Sync to Supabase failed")
        except Exception as e:
            logger.error("supabase_sync_failed", error=str(e))
            print(f"[Ingestion] ❌ Sync error: {e}")

        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info("ingestion_job_completed", duration_seconds=duration)
        print(f"[Ingestion] 🏁 Job completed in {duration:.2f}s\n")
        
        return final_content


async def run_ingestion_job():
    """Entry point for scheduled job."""
    job = IngestionJob()
    await job.run()


if __name__ == "__main__":
    # Manual run for testing
    asyncio.run(run_ingestion_job())
