import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.jobs.ingestion import IngestionJob
from app.services.index_pool import IndexPoolService

@pytest.mark.asyncio
async def test_index_pool_loads_images_json():
    """Test that IndexPoolService loads from images.json."""

    service = IndexPoolService()

    # Mock load_index to return IndexItems
    # Since we changed get_image_ids to call load_index("images"), we mock that.

    mock_items = [
        MagicMock(id="img_1"),
        MagicMock(id="img_2"),
        MagicMock(id="img_3")
    ]

    service.load_index = AsyncMock(return_value=mock_items)

    # Act
    ids = await service.get_image_ids(limit=2)

    # Assert
    assert len(ids) == 2
    assert ids[0] in ["img_1", "img_2", "img_3"]

    # Verify it called load_index with "images"
    service.load_index.assert_called_with("images")
