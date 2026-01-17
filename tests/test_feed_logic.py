import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from starlette.requests import Request
from app.routers.feed import get_feed
from app.models.response import FeedType, FeedResponse

def create_mock_request():
    """Create a real Request object with dummy scope to satisfy slowapi."""
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
async def test_feed_has_more_logic_with_mixed_content():
    """
    Test that hasMore is True even if item count > limit (e.g., due to images).
    """

    # Mock dependencies
    mock_request = create_mock_request()
    mock_current_user = {"uid": "test-user"}

    # Mock services
    mock_generator = AsyncMock()
    # Return 13 IDs (10 videos + 3 images) when limit is 10
    mock_generator.generate.return_value = (["id"] * 13, "next_cursor")

    mock_hydrator = AsyncMock()
    # Return 13 hydrated items
    mock_hydrator.hydrate.return_value = [{"id": "item"} for _ in range(13)]

    mock_firestore = AsyncMock()
    mock_firestore.load_user_context.return_value = MagicMock()

    with patch("app.routers.feed.get_services", return_value=(None, None, mock_generator, mock_hydrator)), \
         patch("app.routers.feed.load_user_context", return_value=MagicMock()):

        response = await get_feed(
            request=mock_request,
            feed_type=FeedType.TRENDING,
            limit=10,
            current_user=mock_current_user
        )

        assert isinstance(response, FeedResponse)
        assert response.meta.item_count == 13
        assert response.meta.limit == 10
        # This is the key assertion: hasMore should be True because 13 >= 10
        assert response.meta.has_more is True

@pytest.mark.asyncio
async def test_feed_has_more_false_when_under_limit():
    """
    Test that hasMore is False when fewer items are returned.
    """

    # Mock services
    mock_generator = AsyncMock()
    # Return 5 IDs when limit is 10
    mock_generator.generate.return_value = (["id"] * 5, "next_cursor")

    mock_hydrator = AsyncMock()
    mock_hydrator.hydrate.return_value = [{"id": "item"} for _ in range(5)]

    with patch("app.routers.feed.get_services", return_value=(None, None, mock_generator, mock_hydrator)), \
         patch("app.routers.feed.load_user_context", return_value=MagicMock()):

        response = await get_feed(
            request=create_mock_request(),
            feed_type=FeedType.TRENDING,
            limit=10,
            current_user={"uid": "test-user"}
        )

        assert response.meta.item_count == 5
        assert response.meta.has_more is False
