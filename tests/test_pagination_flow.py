import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from starlette.requests import Request
from app.routers.feed import get_feed
from app.models.response import FeedType, FeedResponse
from app.services.deduplication import DeduplicationService
from app.services.generator import FeedGenerator

def create_mock_request():
    """Create a real Request object with dummy scope."""
    scope = {
        "type": "http",
        "client": ("127.0.0.1", 80),
        "headers": [],
        "scheme": "http",
        "method": "GET",
        "path": "/feed",
        "query_string": b"",
        "app": MagicMock()
    }
    return Request(scope)

@pytest.mark.asyncio
async def test_pagination_flow():
    """
    Simulate a user scrolling through the feed (Page 1 -> Page 2).
    Verify that items from Page 1 are not repeated in Page 2.
    """

    # 1. Setup Services with real logic where possible

    # Mock Redis (Async)
    mock_redis = AsyncMock()
    # Use a set to simulate Redis storage
    redis_storage = {}

    async def mock_sadd(key, *values):
        if key not in redis_storage:
            redis_storage[key] = set()
        redis_storage[key].update(values)
        return len(values)

    async def mock_smembers(key):
        return redis_storage.get(key, set())

    async def mock_expire(key, ttl):
        return True

    mock_redis.sadd.side_effect = mock_sadd
    mock_redis.smembers.side_effect = mock_smembers
    mock_redis.expire.side_effect = mock_expire

    # Real Deduplication Service with Mock Redis
    dedup_service = DeduplicationService(redis_client=mock_redis)

    # Mock Index Pool (returns a predictable list of IDs)
    mock_index_pool = AsyncMock()
    # Let's say we have 20 trending items: item_0 to item_19
    all_trending_ids = [f"item_{i}" for i in range(20)]
    mock_index_pool.get_trending_ids.return_value = all_trending_ids

    # Use real Generator with mocked index pool and real dedup
    # We need to mock settings for mixing ratios
    with patch("app.services.generator.get_settings") as mock_settings:
        mock_settings.return_value.trending_ratio = 1.0 # 100% trending for simplicity

        generator = FeedGenerator(mock_index_pool, dedup_service, mock_redis)

        # Override _mix_images_into_feed to avoid adding images (simpler test)
        generator._mix_images_into_feed = lambda v, i: v

        # Mock Hydrator (just returns dummy items for IDs)
        mock_hydrator = AsyncMock()
        mock_hydrator.hydrate.side_effect = lambda ids: [{"id": i, "title": f"Title {i}"} for i in ids]

        # Mock Firestore (User Context)
        mock_firestore = AsyncMock()
        mock_user_context = MagicMock()
        mock_user_context.uid = "test-user"
        mock_user_context.seen_ids = [] # Long term history empty
        mock_firestore.load_user_context.return_value = mock_user_context

        # Patch get_services to return our assembled stack
        with patch("app.routers.feed.get_services", return_value=(mock_index_pool, dedup_service, generator, mock_hydrator)), \
             patch("app.routers.feed.load_user_context", return_value=mock_user_context):

            # --- PAGE 1 REQUEST ---
            response_p1 = await get_feed(
                request=create_mock_request(),
                feed_type=FeedType.TRENDING,
                limit=5,
                current_user={"uid": "test-user"}
            )

            # Verify Page 1
            assert len(response_p1.feed) == 5
            page_1_ids = [item["id"] for item in response_p1.feed]
            print(f"Page 1 IDs: {page_1_ids}")

            # Cursor should be present
            cursor_p1 = response_p1.meta.cursor
            assert cursor_p1 is not None

            # Verify items were marked in Redis (DeduplicationService logic)
            # The session ID is encoded in the cursor
            session_id, _ = dedup_service.decode_cursor(cursor_p1)
            saved_in_redis = await mock_redis.smembers(f"session:{session_id}")
            assert len(saved_in_redis) == 5
            assert set(page_1_ids) == saved_in_redis

            # --- PAGE 2 REQUEST (Passing cursor) ---
            response_p2 = await get_feed(
                request=create_mock_request(),
                feed_type=FeedType.TRENDING,
                limit=5,
                cursor=cursor_p1, # Pass cursor from P1
                current_user={"uid": "test-user"}
            )

            # Verify Page 2
            assert len(response_p2.feed) == 5
            page_2_ids = [item["id"] for item in response_p2.feed]
            print(f"Page 2 IDs: {page_2_ids}")

            # CRITICAL: Page 2 should NOT contain any items from Page 1
            # If dedup works, generator filters out what's in Redis
            common_ids = set(page_1_ids).intersection(set(page_2_ids))
            assert len(common_ids) == 0, f"Duplicates found: {common_ids}"

            # Verify session ID persisted
            cursor_p2 = response_p2.meta.cursor
            session_id_p2, _ = dedup_service.decode_cursor(cursor_p2)
            assert session_id_p2 == session_id
