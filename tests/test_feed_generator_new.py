import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.generator import FeedGenerator
from app.services.deduplication import DeduplicationService
from app.models.user import UserContext, UserPreferences

@pytest.mark.asyncio
async def test_feed_plan_generation():
    # Setup Mocks
    mock_redis = AsyncMock()
    mock_index_pool = AsyncMock()

    # Mock index pool returns dummy IDs
    mock_index_pool.get_trending_ids.return_value = [f"trend_{i}" for i in range(100)]
    mock_index_pool.get_image_ids.return_value = [f"img_{i}" for i in range(10)]

    # Dedup Service
    dedup_service = DeduplicationService(redis_client=mock_redis)

    # Generator
    generator = FeedGenerator(mock_index_pool, dedup_service, redis_client=mock_redis)

    # Mock internal fallback service to prevent real calls
    generator.fallback = MagicMock()
    generator.fallback.get_personalized_fallback = AsyncMock(return_value=[])
    generator.fallback.is_cold_start_friends.return_value = False
    generator.fallback.get_friend_fallback = AsyncMock(return_value=[])

    # Mock internal methods to simplify test
    with patch.object(generator, '_get_personalized_candidates', return_value=[]), \
         patch.object(generator, '_get_friend_candidates', return_value=[]):

        # Test 1: First Request (Generate)
        user_context = UserContext(uid="user1", preferences=UserPreferences(), friendIds=[], seenIds=[], favorites=[], watchlist=[])

        # Mock Redis lrange logic
        # First call: _get_from_plan -> returns empty (plan not exists)
        # Second call: _get_from_plan (at end) -> returns slice of items we just pushed
        # We simulate the rpush effect by having lrange return data on 2nd call
        mock_redis.lrange.side_effect = [[], [f"trend_{i}" for i in range(10)]]

        items, cursor = await generator.generate(user_context, limit=10)

        # Validation
        assert len(items) == 10
        assert items[0] == "trend_0"

        # Verify Redis calls
        # We expect rpush to be called to save the plan
        assert mock_redis.rpush.called
        # We expect lrange to be called twice (check, then fetch)
        assert mock_redis.lrange.call_count == 2

@pytest.mark.asyncio
async def test_feed_plan_pagination_hit():
    # Setup
    mock_redis = AsyncMock()
    mock_index_pool = AsyncMock()
    dedup_service = DeduplicationService(redis_client=mock_redis)
    generator = FeedGenerator(mock_index_pool, dedup_service, redis_client=mock_redis)

    user_context = UserContext(uid="user1", preferences=UserPreferences(), friendIds=[], seenIds=[], favorites=[], watchlist=[])

    # Test 2: Pagination (Hit Plan)
    # Mock Redis lrange to return enough items immediately
    # We request limit=10. Mock returns 10 items.
    mock_redis.lrange.return_value = [f"item_{i}" for i in range(10, 20)]

    cursor = dedup_service.encode_cursor("sess1", 10)

    items, next_cursor = await generator.generate(user_context, limit=10, cursor=cursor)

    assert len(items) == 10
    assert items[0] == "item_10"

    # Verify NO generation triggered (rpush not called)
    assert not mock_redis.rpush.called

@pytest.mark.asyncio
async def test_feed_plan_extension():
    # Setup
    mock_redis = AsyncMock()
    mock_index_pool = AsyncMock()
    # Mock trending to ensure we have content to generate
    mock_index_pool.get_trending_ids.return_value = [f"new_{i}" for i in range(50)]
    mock_index_pool.get_image_ids.return_value = []

    dedup_service = DeduplicationService(redis_client=mock_redis)
    generator = FeedGenerator(mock_index_pool, dedup_service, redis_client=mock_redis)

    generator.fallback = MagicMock()
    generator.fallback.get_personalized_fallback = AsyncMock(return_value=[])

    user_context = UserContext(uid="user1", preferences=UserPreferences(), friendIds=[], seenIds=[], favorites=[], watchlist=[])

    # Test 3: Plan Extension (Partial Hit)
    # Request limit=10. Plan has only 5 items left.
    # lrange call 1: returns 5 items.
    # We trigger generation.
    # rpush called.
    # lrange call 2: returns 10 items (simulated).

    mock_redis.lrange.side_effect = [
        [f"old_{i}" for i in range(5)],  # Initial check: 5 items
        [f"old_{i}" for i in range(5)] + [f"new_{i}" for i in range(5)] # Final fetch: 10 items
    ]

    cursor = dedup_service.encode_cursor("sess1", 50)

    # Mock internal methods to isolate logic
    with patch.object(generator, '_get_personalized_candidates', return_value=[]), \
         patch.object(generator, '_get_friend_candidates', return_value=[]):

        items, next_cursor = await generator.generate(user_context, limit=10, cursor=cursor)

        assert len(items) == 10
        assert items[0] == "old_0"
        assert items[5] == "new_0"

        # Verify generation triggered
        assert mock_redis.rpush.called
