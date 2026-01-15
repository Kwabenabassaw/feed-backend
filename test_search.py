"""
Test script for Search API.
"""

import asyncio
import os
import sys
import json
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.search_service import get_search_service


async def test_search():
    """Test the search service."""
    print("\n" + "="*60)
    print("Testing Search Service")
    print("="*60)
    
    # 1. Setup mock data
    print("\n1. Setting up mock index...")
    service = get_search_service()
    
    # Inject mock data directly
    service._index = [
        {"id": "1", "title": "The Dark Knight", "mediaType": "movie"},
        {"id": "2", "title": "Dark City", "mediaType": "movie"},
        {"id": "3", "title": "Knight Rider", "mediaType": "tv"},
        {"id": "4", "title": "Batman Begins", "mediaType": "movie"},
        {"id": "5", "title": "Stranger Things", "mediaType": "tv"},
        {"id": "6", "title": "Inception Explained in 1 Minute", "mediaType": "short"},
    ]
    service._build_search_map()
    print(f"   Mock index created with {len(service._index)} items")
    
    # 2. Test exact match
    print("\n2. Testing exact match 'The Dark Knight'...")
    results = await service.search("The Dark Knight")
    print(f"   Found {len(results)} results")
    if results and results[0]["title"] == "The Dark Knight":
        print("   ✅ Exact match found first")
    else:
        print("   ❌ Exact match failed")
        
    # 3. Test simple prefix/partial 'Dark'
    print("\n3. Testing partial match 'Dark'...")
    results = await service.search("Dark")
    titles = [r["title"] for r in results]
    print(f"   Found: {titles}")
    if "The Dark Knight" in titles and "Dark City" in titles:
        print("   ✅ Partial match successful")
    else:
        print("   ❌ Partial match failed")
        
    # 4. Test movie filter
    print("\n4. Testing filter type='tv' for 'Knight'...")
    results = await service.search("Knight", media_type="tv")
    titles = [r["title"] for r in results]
    print(f"   Found: {titles}")
    if "Knight Rider" in titles and "The Dark Knight" not in titles:
        print("   ✅ Filter successful")
    else:
        print("   ❌ Filter failed")
        
    print("\n" + "="*60)
    print("Search Service Test Completed")
    print("="*60)
    return True

if __name__ == "__main__":
    asyncio.run(test_search())
