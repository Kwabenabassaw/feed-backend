"""
Feed Item Models

Defines the unified feed item schema for both indexing (lightweight)
and hydration (full metadata).
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


class ContentType(str, Enum):
    """Content type categories."""
    TRAILER = "trailer"
    TEASER = "teaser"
    CLIP = "clip"
    BTS = "bts"
    INTERVIEW = "interview"
    FEATURETTE = "featurette"
    SHORT = "short"
    COMMUNITY = "community"
    IMAGE = "image"


class VideoType(str, Enum):
    """Video source type — includes content subtypes produced by the ingestion pipeline."""
    YOUTUBE = "youtube"
    EXTERNAL = "external"
    TRAILER = "trailer"
    TEASER = "teaser"
    CLIP = "clip"
    BTS = "bts"
    INTERVIEW = "interview"
    FEATURETTE = "featurette"
    SHORT = "short"
    IMAGE = "image"
    BEHIND_THE_SCENES = "behind_the_scenes"


class IndexItem(BaseModel):
    """
    Lightweight item for index storage.
    
    Used in genre_*.json and global_trending.json files.
    Memory-efficient: only stores what's needed for selection.
    """
    id: str = Field(..., description="Unique item ID (YouTube video ID or internal)")
    score: float = Field(..., description="Pre-calculated relevance score (0-100)")
    tags: List[str] = Field(default_factory=list, description="Genre/content tags")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="When item was indexed")
    tmdb_id: Optional[int] = Field(None, alias="tmdbId", description="TMDB ID if linked")
    media_type: Optional[str] = Field(None, alias="mediaType", description="movie or tv")
    
    model_config = ConfigDict(populate_by_name=True)


class FeedItem(BaseModel):
    """
    Full feed item returned to client after hydration.
    
    Contains all metadata needed for display in the mobile app.
    """
    id: str = Field(..., description="Unique item ID")
    tmdb_id: Optional[int] = Field(None, alias="tmdbId")
    media_type: str = Field(default="movie", alias="mediaType")
    
    # Display metadata
    title: str = Field(..., description="Content title")
    overview: Optional[str] = Field(None, description="Description/synopsis")
    poster_path: Optional[str] = Field(None, alias="posterPath")
    backdrop_path: Optional[str] = Field(None, alias="backdropPath")
    
    # Video data
    youtube_key: Optional[str] = Field(None, alias="youtubeKey", description="YouTube video ID")
    video_type: VideoType = Field(default=VideoType.YOUTUBE, alias="videoType")
    content_type: ContentType = Field(default=ContentType.TRAILER, alias="contentType")
    duration: Optional[int] = Field(None, description="Video duration in seconds")
    video_name: Optional[str] = Field(None, alias="videoName", description="Video name")
    
    # Image / thumbnail data (required for image feed items)
    image_url: Optional[str] = Field(None, alias="imageUrl", description="Full image URL for image content")
    thumbnail_url: Optional[str] = Field(None, alias="thumbnailUrl", description="Thumbnail URL")
    poster: Optional[str] = Field(None, description="Full poster URL")
    channel_title: Optional[str] = Field(None, alias="channelTitle", description="Channel/creator name")
    
    # Metadata
    source: str = Field(default="trending", description="trending, genre, friend, community")
    reason: Optional[str] = Field(None, description="Why this was recommended")
    genres: List[str] = Field(default_factory=list)
    popularity: float = Field(default=0.0)
    vote_average: Optional[float] = Field(None, alias="voteAverage")
    release_date: Optional[str] = Field(None, alias="releaseDate")
    feed_type: Optional[str] = Field(None, alias="feedType", description="trending, following, for_you")
    
    # Tracking
    score: float = Field(default=0.0, description="Ranking score (internal)")
    freshness: Optional[str] = Field(None, description="new, fresh, standard")
    
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)


class ContentDictionary(BaseModel):
    """
    Master content dictionary entry.
    
    Stored in Supabase for hydration lookups.
    """
    id: str
    tmdb_id: Optional[int] = Field(None, alias="tmdbId")
    media_type: str = Field(default="movie", alias="mediaType")
    title: str
    overview: Optional[str] = None
    poster_path: Optional[str] = Field(None, alias="posterPath")
    backdrop_path: Optional[str] = Field(None, alias="backdropPath")
    youtube_key: Optional[str] = Field(None, alias="youtubeKey")
    video_type: str = Field(default="trailer", alias="videoType")
    content_type: Optional[str] = Field(None, alias="contentType")
    image_url: Optional[str] = Field(None, alias="imageUrl")
    poster: Optional[str] = Field(None)
    genres: List[str] = Field(default_factory=list)
    popularity: float = Field(default=0.0)
    vote_average: Optional[float] = Field(None, alias="voteAverage")
    release_date: Optional[str] = Field(None, alias="releaseDate")
    indexed_at: datetime = Field(default_factory=datetime.utcnow, alias="indexedAt")
    
    model_config = ConfigDict(populate_by_name=True)
