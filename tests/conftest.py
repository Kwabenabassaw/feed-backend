"""
Pytest Fixtures

Shared mocks and fixtures for testing.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock
from typing import List, Dict, Any

from app.models.user import UserContext, UserPreferences
from app.models.feed_item import FeedItem

@pytest.fixture
def mock_user_context():
    """Create a sample user context."""
    return UserContext(
        uid="test_user_123",
        preferences=UserPreferences(
            selectedGenres=["Action", "Sci-Fi"],
            selectedGenreIds=[28, 878],
            streamingProviders=["Netflix"]
        ),
        friendIds=["friend_1", "friend_2"],
        seenIds=["seen_1", "seen_2"],
        favorites=["fav_1"],
        watchlist=["watch_1"]
    )

@pytest.fixture
def mock_feed_items() -> List[Dict[str, Any]]:
    """Create sample feed items."""
    return [
        {
            "id": "item_1",
            "title": "Action Movie",
            "mediaType": "movie",
            "genreIds": [28],
            "releaseDate": "2025-01-01",
            "popularity": 100.0,
            "voteAverage": 8.0,
            "provider_names": ["Netflix"]
        },
        {
            "id": "item_2",
            "title": "Sci-Fi Show",
            "mediaType": "tv",
            "genreIds": [878],
            "releaseDate": "2025-01-02",
            "popularity": 90.0,
            "voteAverage": 7.5,
            "provider_names": ["Hulu"]
        },
        {
            "id": "trending_1",
            "title": "Trending Hit",
            "mediaType": "movie",
            "genreIds": [18],
            "releaseDate": "2025-01-03",
            "popularity": 500.0,
            "voteAverage": 9.0,
            "provider_names": ["Netflix"]
        },
        {
            "id": "seen_1",
            "title": "Seen Movie",
            "mediaType": "movie",
            "genreIds": [28],
            "popularity": 80.0
        }
    ]

@pytest.fixture
def mock_index_pool(mock_feed_items):
    """Mock IndexPoolService."""
    mock = MagicMock()
    mock.get_pool.return_value = mock_feed_items
    mock.get_all_items.return_value = mock_feed_items
    return mock

@pytest.fixture
def mock_dedup_service():
    """Mock DeduplicationService."""
    mock = MagicMock()
    mock.filter_seen_items.side_effect = lambda items, context, **kwargs: [
        i for i in items if i["id"] not in context.seenIds
    ]
    return mock
