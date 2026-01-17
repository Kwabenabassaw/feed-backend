import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.index_pool import IndexPoolService

@pytest.mark.asyncio
async def test_image_ids_caching():
    """Test that image IDs are cached and don't trigger repeated network fetches."""

    service = IndexPoolService()
    service.settings = MagicMock()
    service.settings.supabase_url = "https://test.supabase.co"
    service.settings.supabase_key = "test-key"

    mock_data = [
        {"id": "img_1", "contentType": "image"},
        {"id": "img_2", "contentType": "image"},
        {"id": "vid_1", "contentType": "trailer"}
    ]

    # Mock httpx response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_data

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        # First call: Should fetch
        ids1 = await service.get_image_ids(limit=10)
        assert len(ids1) == 2
        assert "img_1" in ids1
        assert "img_2" in ids1
        assert mock_client.get.call_count == 1

        # Second call: Should use cache (no new fetch)
        ids2 = await service.get_image_ids(limit=10)
        assert len(ids2) == 2
        assert mock_client.get.call_count == 1  # Count should remain 1
