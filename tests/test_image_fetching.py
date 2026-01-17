import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.jobs.ingestion import IngestionJob

@pytest.mark.asyncio
async def test_fetch_image_feed_items():
    """
    Test fetching image feed items from TMDB.
    """

    # Mock data
    mock_tmdb_trending = {
        "results": [
            {
                "id": 100,
                "title": "Movie 1",
                "poster_path": "/poster1.jpg",
                "overview": "Overview 1",
                "vote_average": 8.0,
                "release_date": "2023-01-01"
            },
            {
                "id": 101,
                "title": "Movie 2"
                # Missing fields should be handled
            }
        ]
    }

    mock_tmdb_images = [
        {
            "url": "https://image.tmdb.org/t/p/original/backdrop1.jpg",
            "type": "backdrop",
            "width": 1920,
            "height": 1080,
            "aspectRatio": 1.78
        }
    ]

    # Mock httpx client
    mock_response_trending = MagicMock()
    mock_response_trending.status_code = 200
    mock_response_trending.json.return_value = mock_tmdb_trending

    # Mock settings
    with patch("app.jobs.ingestion.settings") as mock_settings:
        mock_settings.tmdb_api_key = "test_key"

        job = IngestionJob()
        job.fetch_tmdb_images = AsyncMock(return_value=mock_tmdb_images)

        # We need to mock httpx.AsyncClient for the initial trending fetch
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.side_effect = [mock_response_trending]
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            # Act
            results = await job.fetch_image_feed_items()

            # Assert
            assert len(results) == 2

            item1 = results[0]
            assert item1["contentType"] == "image"
            assert item1["id"] == "img_100"
            assert item1["imageUrl"] == "https://image.tmdb.org/t/p/original/backdrop1.jpg"
            assert item1["tmdbId"] == 100
            assert item1["mediaType"] == "movie"
            assert item1["youtubeKey"] is None

            # Verify call to fetch_tmdb_images
            job.fetch_tmdb_images.assert_called()

@pytest.mark.asyncio
async def test_fetch_image_feed_items_no_images():
    """Test behavior when no images are found for a movie."""

    mock_tmdb_trending = {"results": [{"id": 100, "title": "Movie 1"}]}

    # Mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_tmdb_trending

    with patch("app.jobs.ingestion.settings") as mock_settings:
        mock_settings.tmdb_api_key = "test_key"

        job = IngestionJob()
        job.fetch_tmdb_images = AsyncMock(return_value=[]) # Return empty list

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            results = await job.fetch_image_feed_items()

            # Should be empty because we skip if no images
            assert len(results) == 0
