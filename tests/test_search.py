"""
Tests for Search Service
"""

import pytest
from app.services.search_service import SearchService

@pytest.fixture
def search_service():
    """Create search service with mock index."""
    service = SearchService()
    service._index = [
        {"id": "1", "title": "The Dark Knight"},
        {"id": "2", "title": "Dark City"},
        {"id": "3", "title": "Knight Rider", "mediaType": "tv"},
        {"id": "4", "title": "Batman Begins"},
        {"id": "5", "title": "Stranger Things", "mediaType": "tv"},
    ]
    service._build_search_map()
    return service

@pytest.mark.asyncio
async def test_exact_match(search_service):
    """Exact title match should return first."""
    results = await search_service.search("The Dark Knight")
    assert results[0]["title"] == "The Dark Knight"

@pytest.mark.asyncio
async def test_partial_match(search_service):
    """Partial tokens should find matches."""
    results = await search_service.search("Dark")
    titles = [r["title"] for r in results]
    assert "The Dark Knight" in titles
    assert "Dark City" in titles
    assert "Stranger Things" not in titles

@pytest.mark.asyncio
async def test_media_type_filter(search_service):
    """Should filter by media type."""
    # "Knight" matches "The Dark Knight" (movie) and "Knight Rider" (tv)
    results = await search_service.search("Knight", media_type="tv")
    assert len(results) == 1
    assert results[0]["title"] == "Knight Rider"

@pytest.mark.asyncio
async def test_empty_query(search_service):
    """Empty query should return empty list."""
    results = await search_service.search("")
    assert results == []
    
    results = await search_service.search("   ")
    assert results == []
