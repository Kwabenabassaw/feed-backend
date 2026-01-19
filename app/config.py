"""
Application Configuration

Load settings from environment variables with validation.
"""

from functools import lru_cache
from typing import Optional
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    # Environment
    environment: str = "development"
    debug: bool = True
    
    # Firebase
    firebase_credentials_path: str = "./service-account.json"
    
    # Supabase
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_service_key: str = ""  # Service role key for auth sync
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # External APIs
    youtube_api_key: Optional[str] = None
    tmdb_api_key: Optional[str] = None
    tmdb_read_access_token: Optional[str] = None
    
    # Rate Limiting
    rate_limit_per_minute: int = 60
    
    # Feed Configuration
    feed_page_size: int = 80
    trending_ratio: float = 0.5      # 50%
    personalized_ratio: float = 0.3  # 30%
    friend_ratio: float = 0.2        # 20%
    
    # Quota Management
    youtube_daily_quota_limit: int = 9000
    
    # Session TTL (seconds)
    session_ttl_seconds: int = 600  # 10 minutes
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
