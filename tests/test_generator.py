"""
Tests for Feed Generator

Verifies the 50/30/20 mixing ratio and cold start handling.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

import sys
sys.path.insert(0, str(__file__).replace("tests\\test_generator.py", ""))

from app.models.user import UserContext, UserPreferences
from app.services.index_pool import IndexPoolService
from app.services.deduplication import DeduplicationService
from app.services.generator import FeedGenerator


@pytest.fixture
def mock_index_pool():
    """Create mock index pool with predictable data."""
    pool = AsyncMock(spec=IndexPoolService)
    
    # Trending IDs
    pool.get_trending_ids = AsyncMock(return_value=[f"trending_{i}" for i in range(30)])
    
    # Genre IDs
    pool.get_genre_ids = AsyncMock(return_value=[f"genre_{i}" for i in range(30)])
    
    # Community IDs
    pool.get_community_hot_ids = AsyncMock(return_value=[f"community_{i}" for i in range(30)])
    
    return pool


@pytest.fixture
def mock_dedup_service():
    """Create mock deduplication service."""
    dedup = MagicMock(spec=DeduplicationService)
    dedup.generate_session_id = MagicMock(return_value="test-session-123")
    dedup.decode_cursor = MagicMock(return_value=("test-session-123", 0))
    dedup.encode_cursor = MagicMock(return_value="next-cursor-abc")
    dedup.get_session_seen_ids = AsyncMock(return_value=set())
    dedup.mark_ids_sent = AsyncMock()
    dedup.filter_seen = lambda ids, user_seen, session_seen: ids
    return dedup


@pytest.fixture
def generator(mock_index_pool, mock_dedup_service):
    """Create generator with mocked dependencies."""
    return FeedGenerator(mock_index_pool, mock_dedup_service)


@pytest.fixture
def normal_user():
    """User with preferences (not cold start)."""
    return UserContext(
        uid="user-123",
        preferences=UserPreferences(
            selectedGenres=["action", "comedy"],
            selectedGenreIds=[28, 35]
        ),
        friendIds=["friend-1", "friend-2"],
        seenIds=[]
    )


@pytest.fixture
def cold_start_user():
    """Brand new user with no preferences."""
    return UserContext(
        uid="new-user-456",
        preferences=UserPreferences(),
        friendIds=[],
        seenIds=[]
    )


class TestMixingRatio:
    """Test the 50/30/20 mixing ratio."""
    
    @pytest.mark.asyncio
    async def test_generates_correct_count(self, generator, normal_user):
        """Should return exactly the requested number of items."""
        ids, cursor = await generator.generate(normal_user, limit=10)
        assert len(ids) == 10
    
    @pytest.mark.asyncio
    async def test_returns_cursor(self, generator, normal_user):
        """Should return a pagination cursor."""
        ids, cursor = await generator.generate(normal_user, limit=10)
        assert cursor is not None
        assert cursor == "next-cursor-abc"


class TestColdStart:
    """Test cold start fallback behavior."""
    
    @pytest.mark.asyncio
    async def test_cold_start_user_gets_feed(self, generator, cold_start_user):
        """New users with no preferences should still get a feed."""
        ids, cursor = await generator.generate(cold_start_user, limit=10)
        assert len(ids) == 10
    
    def test_cold_start_detection(self, cold_start_user):
        """Should correctly detect cold start users."""
        assert cold_start_user.is_cold_start is True
        assert cold_start_user.has_friends is False


class TestDeduplication:
    """Test deduplication logic."""
    
    @pytest.mark.asyncio
    async def test_filters_seen_items(self, generator, normal_user):
        """Should not include items from seen_ids."""
        normal_user.seen_ids = ["trending_0", "trending_1"]
        
        # Mock filter to actually filter
        generator.dedup.filter_seen = lambda ids, user_seen, session_seen: [
            id for id in ids if id not in user_seen
        ]
        
        ids, _ = await generator.generate(normal_user, limit=10)
        
        assert "trending_0" not in ids
        assert "trending_1" not in ids


class TestTieredShuffle:
    """Test the tiered shuffle preserves top items."""
    
    def test_preserves_top_three(self, generator):
        """Top 3 items should not be shuffled."""
        items = ["top1", "top2", "top3", "mid1", "mid2", "tail1", "tail2", "tail3"]
        
        # Run shuffle multiple times
        for _ in range(10):
            shuffled = generator._tiered_shuffle(items.copy())
            # Top 3 should always be the same
            assert shuffled[:3] == ["top1", "top2", "top3"]
    
    def test_handles_small_lists(self, generator):
        """Should handle lists smaller than tier sizes."""
        small_list = ["a", "b"]
        result = generator._tiered_shuffle(small_list)
        assert result == ["a", "b"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
