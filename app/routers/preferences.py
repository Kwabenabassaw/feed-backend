"""
Preference Router

Endpoints for syncing user preferences to Supabase.
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Body, HTTPException
from pydantic import BaseModel

from ..core.security import get_current_user
from ..core.logging import get_logger
from ..services.preference_service import get_preference_service, PreferenceService

router = APIRouter(prefix="/user/preferences", tags=["preferences"])
logger = get_logger(__name__)

class SyncPreferencesRequest(BaseModel):
    selectedGenres: Optional[List[int]] = None
    selectedGenreIds: Optional[List[int]] = None # Handle legacy param naming if needed
    streamingProviders: Optional[List[Dict[str, Any]]] = None
    selectedMovies: Optional[List[Dict[str, Any]]] = None # Seed content
    selectedShows: Optional[List[Dict[str, Any]]] = None # Seed content

@router.post("/sync")
async def sync_preferences(
    request: SyncPreferencesRequest,
    current_user: dict = Depends(get_current_user),
    service: PreferenceService = Depends(get_preference_service)
):
    """
    Sync preferences from Client/Firebase to Supabase.
    
    This is a fire-and-forget sync endpoint.
    Firebase remains the Source of Truth for the client.
    Supabase is updated for feed generation.
    """
    user_id = current_user["uid"]
    
    # Handle genres (support both field names just in case)
    genre_ids = request.selectedGenreIds or request.selectedGenres
    if genre_ids is not None:
        await service.sync_genre_preferences(user_id, genre_ids)
        
    # Handle providers
    if request.streamingProviders is not None:
        await service.sync_provider_preferences(user_id, request.streamingProviders)
        
    # Handle seed content (movies/shows)
    if request.selectedMovies is not None or request.selectedShows is not None:
        await service.sync_seed_content(
            user_id, 
            request.selectedMovies or [], 
            request.selectedShows or []
        )
        
    return {"status": "synced", "uid": user_id}
