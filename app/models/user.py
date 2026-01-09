"""
User Models

User preferences and context for feed personalization.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class UserPreferences(BaseModel):
    """
    User preferences loaded from Firestore.
    
    Used for personalization in the 30% genre bucket.
    """
    selected_genres: List[str] = Field(
        default_factory=list, 
        alias="selectedGenres",
        description="User's preferred genres"
    )
    selected_genre_ids: List[int] = Field(
        default_factory=list,
        alias="selectedGenreIds",
        description="TMDB genre IDs"
    )
    streaming_providers: List[str] = Field(
        default_factory=list,
        alias="streamingProviders",
        description="User's streaming services"
    )
    
    class Config:
        populate_by_name = True


class FriendInfo(BaseModel):
    """Basic friend information."""
    uid: str
    username: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    
    class Config:
        populate_by_name = True


class UserContext(BaseModel):
    """
    Complete user context for feed generation.
    
    Aggregated from multiple Firestore collections.
    """
    uid: str
    preferences: UserPreferences = Field(default_factory=UserPreferences)
    friend_ids: List[str] = Field(
        default_factory=list,
        alias="friendIds",
        description="List of friend UIDs"
    )
    seen_ids: List[str] = Field(
        default_factory=list,
        alias="seenIds",
        description="Recently seen item IDs (for deduplication)"
    )
    favorites: List[str] = Field(
        default_factory=list,
        description="User's favorite content IDs"
    )
    watchlist: List[str] = Field(
        default_factory=list,
        description="User's watchlist IDs"
    )
    
    # Cold start detection
    @property
    def is_cold_start(self) -> bool:
        """Check if user has no preferences (new user)."""
        return len(self.preferences.selected_genres) == 0
    
    @property
    def has_friends(self) -> bool:
        """Check if user has any friends."""
        return len(self.friend_ids) > 0
    
    class Config:
        populate_by_name = True
