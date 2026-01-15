"""
Search Service

In-memory search service for fast content lookups.
Loads 'indexes/master_content.json' into memory and provides
fast, case-insensitive partial matching.
"""

import json
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..core.logging import get_logger

logger = get_logger(__name__)


class SearchService:
    """
    Fast in-memory search for feed content.
    """
    
    def __init__(self):
        self._index: List[Dict[str, Any]] = []
        self._last_loaded: Optional[datetime] = None
        self._is_loading = False
        
        # Pre-computed search terms for faster lookup
        # Map: "term" -> [list of item indices]
        self._search_map: Dict[str, List[int]] = {}
    
    async def initialize(self):
        """Load index from disk."""
        if self._index:
            return
            
        await self.reload_index()

    async def reload_index(self):
        """Reload index from disk."""
        if self._is_loading:
            return
            
        self._is_loading = True
        try:
            indexes_dir = Path("indexes")
            master_path = indexes_dir / "master_content.json"
            
            if not master_path.exists():
                logger.warning("search_index_not_found", path=str(master_path))
                self._index = []
                return
                
            def _load():
                with open(master_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            
            # Run IO in thread pool
            data = await asyncio.to_thread(_load)
            
            # Convert dict to list items
            items = list(data.values()) if isinstance(data, dict) else data
            
            self._index = items
            self._last_loaded = datetime.utcnow()
            
            # Build simple search map (lowercase token -> item indices)
            self._build_search_map()
            
            logger.info("search_index_loaded", items=len(self._index))
            
        except Exception as e:
            logger.error("search_index_load_failed", error=str(e))
        finally:
            self._is_loading = False
            
    def _build_search_map(self):
        """Build simple inverted index for faster lookups."""
        new_map = {}
        
        for idx, item in enumerate(self._index):
            title = item.get("title", "").lower()
            
            # Simple tokenization
            tokens = set(title.split())
            
            # Also add full title for exact matches
            tokens.add(title)
            
            for token in tokens:
                # keep tokens minimal length to avoid noise
                if len(token) < 2:
                    continue
                    
                if token not in new_map:
                    new_map[token] = []
                new_map[token].append(idx)
                
        self._search_map = new_map

    async def search(
        self, 
        query: str, 
        limit: int = 20, 
        media_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for items matching query.
        
        Args:
            query: Search string
            limit: Max results
            media_type: Optional filter (movie/tv)
            
        Returns:
            List of matching feed items
        """
        if not self._index:
            await self.initialize()
            
        if not query or len(query.strip()) < 2:
            return []
            
        query = query.lower().strip()
        query_tokens = query.split()
        
        # 1. Candidate selection using inverted index (if possible)
        # For now, we'll do a simple linear scan if inverted index isn't used perfectly,
        # but let's try to be smart.
        
        # Simple scoring:
        # - Exact title match: 100
        # - Starts with query: 80
        # - Contains query: 50
        # - Partial token match: 10 * num_matches
        
        matches = []
        
        for item in self._index:
            # Filter by type if requested
            if media_type and item.get("mediaType") != media_type:
                continue
                
            title = item.get("title", "").lower()
            score = 0
            
            if title == query:
                score = 100
            elif title.startswith(query):
                score = 80
            elif query in title:
                score = 50
            else:
                # Token matching
                item_tokens = set(title.split())
                match_count = sum(1 for q in query_tokens if any(q in t for t in item_tokens))
                if match_count > 0:
                    score = 10 * match_count
            
            if score > 0:
                matches.append((score, item))
        
        # Sort by score desc
        matches.sort(key=lambda x: x[0], reverse=True)
        
        # Return items
        return [m[1] for m in matches[:limit]]

# Singleton
_search_service: Optional[SearchService] = None

def get_search_service() -> SearchService:
    global _search_service
    if _search_service is None:
        _search_service = SearchService()
    return _search_service
