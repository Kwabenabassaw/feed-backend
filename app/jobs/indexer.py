"""
Indexer Job

Regenerates index files from the master content dictionary.
Calculates scores and organizes into genre buckets.
Runs every 30 minutes.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


# Genre mappings (content tags to index bucket names)
GENRE_MAPPINGS = {
    "action": ["action", "adventure"],
    "comedy": ["comedy"],
    "drama": ["drama"],
    "horror": ["horror"],
    "thriller": ["thriller", "mystery", "crime"],
    "romance": ["romance"],
    "scifi": ["science fiction", "sci-fi", "scifi"],
    "fantasy": ["fantasy"],
    "animation": ["animation", "anime"],
    "documentary": ["documentary"],
}


class IndexerJob:
    """
    Background job for index generation.
    
    Reads master_content.json and generates:
    1. global_trending.json (top 1000 by popularity)
    2. genre_*.json (top 500 per genre bucket)
    """
    
    def __init__(self):
        self.indexes_dir = Path("indexes")
    
    def _calculate_score(self, item: dict) -> float:
        """
        Calculate ranking score for an item.
        
        Formula: BasePopularity * 0.5 + FreshnessBonus
        """
        popularity = item.get("popularity", 0)
        vote_average = item.get("voteAverage", 0)
        
        # Base score from popularity (normalized to 0-50)
        base_score = min(50, popularity / 2)
        
        # Quality bonus from ratings (0-30)
        quality_score = (vote_average or 0) * 3
        
        # Freshness bonus (0-20)
        freshness_score = 10  # TODO: Calculate from releaseDate
        
        return round(base_score + quality_score + freshness_score, 1)
    
    def _get_item_genres(self, item: dict) -> List[str]:
        """Extract normalized genre tags from item."""
        genres = item.get("genres", [])
        if isinstance(genres, str):
            genres = [genres]
        return [g.lower() for g in genres]
    
    def _map_to_buckets(self, item: dict) -> List[str]:
        """Map item genres to index bucket names."""
        item_genres = self._get_item_genres(item)
        buckets = []
        
        for bucket_name, genre_keywords in GENRE_MAPPINGS.items():
            for keyword in genre_keywords:
                if keyword in item_genres:
                    buckets.append(bucket_name)
                    break
        
        return buckets or ["general"]
    
    def _create_index_entry(self, item: dict, score: float) -> dict:
        """Create lightweight index entry from full item."""
        return {
            "id": item.get("id") or item.get("youtubeKey"),
            "score": score,
            "tags": self._get_item_genres(item),
            "timestamp": datetime.utcnow().isoformat(),
            "tmdbId": item.get("tmdbId"),
            "mediaType": item.get("mediaType", "movie"),
        }
    
    async def run(self):
        """
        Run the indexer job.
        
        1. Load master_content.json
        2. Calculate scores for all items
        3. Generate global_trending.json
        4. Generate genre_*.json for each bucket
        5. Save all indexes
        """
        logger.info("indexer_job_started")
        start_time = datetime.utcnow()
        
        # Load content
        content_path = self.indexes_dir / "master_content.json"
        if not content_path.exists():
            logger.warning("no_master_content", path=str(content_path))
            return
        
        try:
            content = json.loads(content_path.read_text())
        except Exception as e:
            logger.error("content_load_failed", error=str(e))
            return
        
        # Score all items
        scored_items = []
        for item in content:
            score = self._calculate_score(item)
            scored_items.append((item, score))
        
        # Sort by score descending
        scored_items.sort(key=lambda x: x[1], reverse=True)
        
        # Generate global trending (top 1000)
        trending = [self._create_index_entry(item, score) for item, score in scored_items[:1000]]
        (self.indexes_dir / "global_trending.json").write_text(json.dumps(trending, indent=2))
        logger.info("index_generated", name="global_trending", count=len(trending))
        
        # Generate genre buckets
        genre_buckets: Dict[str, List[dict]] = {name: [] for name in GENRE_MAPPINGS}
        
        for item, score in scored_items:
            buckets = self._map_to_buckets(item)
            entry = self._create_index_entry(item, score)
            
            for bucket in buckets:
                if bucket in genre_buckets and len(genre_buckets[bucket]) < 500:
                    genre_buckets[bucket].append(entry)
        
        # Save genre indexes
        for genre_name, items in genre_buckets.items():
            if items:
                (self.indexes_dir / f"genre_{genre_name}.json").write_text(json.dumps(items, indent=2))
                logger.info("index_generated", name=f"genre_{genre_name}", count=len(items))
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            "indexer_job_completed",
            total_items=len(content),
            indexes_generated=len(genre_buckets) + 1,
            duration_seconds=duration
        )


async def run_indexer_job():
    """Entry point for scheduled job."""
    job = IndexerJob()
    await job.run()


if __name__ == "__main__":
    # Manual run for testing
    asyncio.run(run_indexer_job())
