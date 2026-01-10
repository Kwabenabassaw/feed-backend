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
                
                # Priority: Trailer > Teaser > Any YouTube video
                for video_type in ["Trailer", "Teaser"]:
                    for video in results:
                        if video.get("site") == "YouTube" and video.get("type") == video_type:
                            return video.get("key")
                
                # Fallback: any YouTube video
                for video in results:
                    if video.get("site") == "YouTube":
                        return video.get("key")
                        
        except Exception as e:
            logger.debug("tmdb_video_fetch_failed", tmdb_id=tmdb_id, error=str(e))
        
        return None
    
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
            
            # Fetch video keys for movies
            for item in movie_items:
                tmdb_id = item["id"]
                youtube_key = await self.fetch_tmdb_video_key(tmdb_id, "movie", client)
                
                # Skip items without YouTube video
                if not youtube_key:
                    logger.debug("tmdb_movie_no_trailer", tmdb_id=tmdb_id)
                    continue
                
                normalized = self._normalize_tmdb_item(item, "movie", youtube_key)
                videos.append(normalized)
                logger.debug("tmdb_movie_processed", tmdb_id=tmdb_id, youtube_key=youtube_key)
            
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
            
            # Fetch video keys for TV shows
            for item in tv_items:
                tmdb_id = item["id"]
                youtube_key = await self.fetch_tmdb_video_key(tmdb_id, "tv", client)
                
                # Skip items without YouTube video
                if not youtube_key:
                    logger.debug("tmdb_tv_no_trailer", tmdb_id=tmdb_id)
                    continue
                
                normalized = self._normalize_tmdb_item(item, "tv", youtube_key)
                videos.append(normalized)
                logger.debug("tmdb_tv_processed", tmdb_id=tmdb_id, youtube_key=youtube_key)
        
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
    
    async def fetch_kinocheck_trailers(self) -> List[dict]:
        """
        Fetch trailers from KinoCheck (trending + latest).
        
        Validates each trailer against TMDB - rejects items without TMDB ID.
        """
        kinocheck = get_kinocheck_service()
        all_trailers = []
        validated = []
        
        # Fetch trending trailers
        try:
            trending = await kinocheck.fetch_trending(limit=30, page=1)
            all_trailers.extend(trending)
            logger.info("kinocheck_trending_raw", count=len(trending))
        except Exception as e:
            logger.warning("kinocheck_trending_failed", error=str(e))
        
        # Fetch latest trailers
        try:
            latest = await kinocheck.fetch_latest(limit=20, page=1)
            all_trailers.extend(latest)
            logger.info("kinocheck_latest_raw", count=len(latest))
        except Exception as e:
            logger.warning("kinocheck_latest_failed", error=str(e))
        
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
        Run the ingestion job.
        
        1. Fetch from all YouTube channels (via RSS - free)
        2. Fetch TMDB trending
        3. Fetch TMDB discover by genre (for variety)
        4. Merge and deduplicate
        5. Save to master_content.json
        """
        logger.info("ingestion_job_started")
        print("\n" + "="*60)
        print("[Ingestion] 🚀 Starting content ingestion...")
        print("="*60)
        start_time = datetime.utcnow()
        
        all_videos = []
        
        # YouTube RSS (free, no quota)
        print("[Ingestion] 📺 Fetching YouTube RSS feeds...")
        for channel_id in YOUTUBE_CHANNELS:
            videos = await self.fetch_youtube_rss(channel_id)
            all_videos.extend(videos)
            logger.info("youtube_rss_fetched", channel=channel_id, count=len(videos))
        print(f"[Ingestion] ✅ YouTube RSS: {len(all_videos)} videos from {len(YOUTUBE_CHANNELS)} channels")
        
        # TMDB trending (uses quota)
        print("[Ingestion] 🎬 Fetching TMDB trending...")
        try:
            tmdb_videos = await self.fetch_tmdb_trending()
            all_videos.extend(tmdb_videos)
            logger.info("tmdb_fetched", count=len(tmdb_videos))
            print(f"[Ingestion] ✅ TMDB Trending: {len(tmdb_videos)} videos with trailers")
        except Exception as e:
            logger.warning("tmdb_fetch_failed", error=str(e))
            print(f"[Ingestion] ⚠️ TMDB Trending failed: {e}")
        
        # TMDB discover by genre (for variety)
        print("[Ingestion] 🎭 Fetching TMDB discover by genre...")
        try:
            discover_videos = await self.fetch_tmdb_discover_by_genre()
            all_videos.extend(discover_videos)
            logger.info("discover_fetched", count=len(discover_videos))
            print(f"[Ingestion] ✅ TMDB Discover: {len(discover_videos)} videos across 10 genres")
        except Exception as e:
            logger.warning("discover_fetch_failed", error=str(e))
            print(f"[Ingestion] ⚠️ TMDB Discover failed: {e}")
        
        # KinoCheck trailers (trending + latest)
        print("[Ingestion] 🎞️ Fetching KinoCheck trailers...")
        try:
            kinocheck_videos = await self.fetch_kinocheck_trailers()
            all_videos.extend(kinocheck_videos)
            logger.info("kinocheck_fetched", count=len(kinocheck_videos))
            print(f"[Ingestion] ✅ KinoCheck: {len(kinocheck_videos)} validated trailers")
        except Exception as e:
            logger.warning("kinocheck_fetch_failed", error=str(e))
            print(f"[Ingestion] ⚠️ KinoCheck failed: {e}")
        
        # Deduplicate by ID
        seen_ids = set()
        unique_videos = []
        for video in all_videos:
            vid_id = video.get("id") or video.get("youtubeKey") or video.get("tmdbId")
            if vid_id and vid_id not in seen_ids:
                seen_ids.add(vid_id)
                unique_videos.append(video)
        
        # Ensure indexes directory exists
        import json
        from pathlib import Path
        indexes_dir = Path("indexes")
        indexes_dir.mkdir(exist_ok=True)
        
        # Load existing content to merge (avoid overwriting)
        master_path = indexes_dir / "master_content.json"
        existing_content = []
        if master_path.exists():
            try:
                existing_content = json.loads(master_path.read_text())
                logger.info("existing_content_loaded", count=len(existing_content))
            except Exception as e:
                logger.warning("existing_content_load_failed", error=str(e))
        
        # Merge: Add new items, update existing
        existing_ids = {
            item.get("id") or item.get("youtubeKey") or item.get("tmdbId")
            for item in existing_content
        }
        
        new_items = [v for v in unique_videos if (v.get("id") or v.get("youtubeKey") or v.get("tmdbId")) not in existing_ids]
        merged_content = existing_content + new_items
        
        # Keep only last 5000 items to prevent unbounded growth
        if len(merged_content) > 5000:
            merged_content = merged_content[-5000:]
        
        # Save to disk
        try:
            master_path.write_text(json.dumps(merged_content, indent=2, default=str))
            logger.info("master_content_saved", path=str(master_path), count=len(merged_content))
            print(f"[Ingestion] ✅ Saved {len(merged_content)} items to {master_path}")
        except Exception as e:
            logger.error("master_content_save_failed", error=str(e))
            print(f"[Ingestion] ❌ Save failed: {e}")
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            "ingestion_job_completed",
            total_items=len(all_videos),
            new_items=len(new_items),
            merged_total=len(merged_content),
            duration_seconds=duration
        )
        
        return merged_content


async def run_ingestion_job():
    """Entry point for scheduled job."""
    job = IngestionJob()
    await job.run()


if __name__ == "__main__":
    # Manual run for testing
    asyncio.run(run_ingestion_job())
