import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.cache_service import CacheService
from app.services.deduplication import DeduplicationService

@pytest.mark.asyncio
async def test_cache_service_async_calls():
    """Test that CacheService awaits Redis calls."""

    # Mock redis client
    mock_redis = AsyncMock()
    mock_redis.get.return_value = '{"test": "data"}'
    mock_redis.setex.return_value = True

    # Patch get_redis_client to return our mock
    with patch("app.services.cache_service.get_redis_client", return_value=mock_redis):
        service = CacheService()

        # Test get
        result = await service.get("test-key")
        assert result == '{"test": "data"}'
        mock_redis.get.assert_awaited_with("test-key")

        # Test set
        await service.set("test-key", "value", 100)
        mock_redis.setex.assert_awaited_with("test-key", 100, "value")

@pytest.mark.asyncio
async def test_deduplication_service_async_redis():
    """Test that DeduplicationService awaits Redis calls."""

    # Mock redis client
    mock_redis = AsyncMock()
    mock_redis.smembers.return_value = {"item1", "item2"}
    mock_redis.sadd.return_value = 1

    service = DeduplicationService(redis_client=mock_redis)

    # Test get_session_seen_ids
    seen = await service.get_session_seen_ids("session-123")
    assert seen == {"item1", "item2"}
    mock_redis.smembers.assert_awaited_with("session:session-123")

    # Test mark_ids_sent
    await service.mark_ids_sent("session-123", ["item3"])
    mock_redis.sadd.assert_awaited_with("session:session-123", "item3")
    mock_redis.expire.assert_awaited()
