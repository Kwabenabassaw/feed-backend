"""
Preference Service

Handles syncing user preferences from Firebase/Frontend to Supabase normalized tables.
"""

from typing import List, Dict, Any, Optional
import httpx
from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

SUPABASE_URL = settings.SUPABASE_URL
SUPABASE_SERVICE_KEY = settings.SUPABASE_SERVICE_KEY

class PreferenceService:
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            logger.error("supabase_not_configured_preference_service")
            
        self.headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        self.base_url = f"{SUPABASE_URL}/rest/v1"

    async def sync_genre_preferences(self, user_id: str, genre_ids: List[int]):
        """
        Sync explicit genre selections.
        
        Strategy:
        1. Delete existing explicit preferences (weight = 1.0) for user.
        2. Insert new selections.
        This preserves implicit preferences (weight != 1.0) if we add them later.
        """
        if not SUPABASE_URL:
            return

        try:
            async with httpx.AsyncClient() as client:
                # 1. Delete existing explicit (weight=1.0)
                delete_url = f"{self.base_url}/user_genre_preferences"
                delete_params = {
                    "user_id": f"eq.{user_id}",
                    "weight": "eq.1.0"
                }
                await client.delete(
                    delete_url, 
                    params=delete_params, 
                    headers=self.headers
                )

                # 2. Insert new
                if genre_ids:
                    insert_payload = [
                        {
                            "user_id": user_id,
                            "genre_id": gid,
                            "weight": 1.0
                        }
                        for gid in genre_ids
                    ]
                    await client.post(
                        f"{self.base_url}/user_genre_preferences",
                        json=insert_payload,
                        headers=self.headers
                    )
            
            logger.info("synced_genres", uid=user_id, count=len(genre_ids))

        except Exception as e:
            logger.error("sync_genres_failed", uid=user_id, error=str(e))
            raise

    async def sync_provider_preferences(self, user_id: str, providers: List[Dict[str, Any]]):
        """
        Sync explicit provider selections.
        
        Providers list expected format: [{"providerId": 123, "providerName": "Netflix", ...}, ...]
        """
        if not SUPABASE_URL:
            return

        try:
            async with httpx.AsyncClient() as client:
                # 1. Delete all providers for user (simplest strategy as providers are boolean list)
                delete_url = f"{self.base_url}/user_provider_preferences"
                await client.delete(
                    delete_url, 
                    params={"user_id": f"eq.{user_id}"}, 
                    headers=self.headers
                )

                # 2. Insert new
                if providers:
                    insert_payload = [
                        {
                            "user_id": user_id,
                            "provider_id": p["providerId"],
                            "provider_name": p["providerName"],
                            "logo_path": p.get("logoPath")
                        }
                        for p in providers
                    ]
                    await client.post(
                        f"{self.base_url}/user_provider_preferences",
                        json=insert_payload,
                        headers=self.headers
                    )
            
            logger.info("synced_providers", uid=user_id, count=len(providers))

        except Exception as e:
            logger.error("sync_providers_failed", uid=user_id, error=str(e))
            raise

    async def sync_seed_content(
        self, 
        user_id: str, 
        movies: List[Dict[str, Any]], 
        shows: List[Dict[str, Any]]
    ):
        """
        Sync onboarding seed content to user_titles using V2 logic.
        
        Strategy:
        - Insert into user_titles table.
        - Set source = 'onboarding_seed'.
        - Set is_favorite = FALSE (it's just a seed, not explicit 'favorite' unless user marked it so).
        - Set status = NULL (unless we want 'watched' implied? No, seed just sets alignment).
        """
        if not SUPABASE_URL:
            return

        all_seeds = movies + shows
        if not all_seeds:
            return

        try:
            from datetime import datetime, timezone
            
            # Prepare payload
            payload = []
            for seed in all_seeds:
                tid = seed.get("id") or seed.get("tmdbId")
                if tid:
                    title_id = str(tid)
                    media_type = seed.get("mediaType", "movie")
                    # Infer media type if missing, usually safe to default but checking helps
                    if "mediaType" not in seed:
                         # Heuristic or pass fallback? API usually sends it.
                         pass
                    
                    payload.append({
                        "user_id": user_id,
                        "title_id": title_id,
                        "media_type": media_type,
                        "title": seed.get("title") or seed.get("name") or "",
                        "poster_path": seed.get("posterPath") or seed.get("poster_path"),
                        "status": None,
                        "is_favorite": False,
                        "rating": None,
                        "source": "onboarding_seed",
                        "added_at": datetime.now(timezone.utc).isoformat(),
                        "synced_at": datetime.now(timezone.utc).isoformat(),
                    })
            
            # Upsert to user_titles
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self.base_url}/user_titles",
                    json=payload,
                    headers={
                        "apikey": SUPABASE_SERVICE_KEY,
                        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                        "Content-Type": "application/json",
                        # UPSERT: merge on conflict
                        "Prefer": "resolution=merge-duplicates,return=minimal"
                    },
                    timeout=10.0
                )
            
            logger.info("synced_seeds", uid=user_id, count=len(payload))
            
        except Exception as e:
            logger.error("sync_seeds_failed", uid=user_id, error=str(e))
            # Don't raise, this is auxiliary data

# Singleton
_preference_service: Optional[PreferenceService] = None

def get_preference_service() -> PreferenceService:
    global _preference_service
    if _preference_service is None:
        _preference_service = PreferenceService()
    return _preference_service
