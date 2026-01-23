"""
User Preferences Migration Script (Phase 5 - V2)

Migrates user preferences from Firebase to normalized Supabase tables.

V2 Logic:
1. `selectedGenres` (Array) -> `user_genre_preferences` (weight=1.0)
2. `streamingProviders` (Array) -> `user_provider_preferences`
3. `selectedMovies` / `selectedShows` -> `user_titles` (seed content)
    - status = NULL
    - is_favorite = FALSE
    - source = 'onboarding_seed'
    - added_at = NOW() (or seed time)

Usage:
    cd feed-backend
    python scripts/migrate_preferences_v2.py
"""

import os
import sys
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "service-account.json")

stats = {
    "users_processed": 0,
    "genres_migrated": 0,
    "providers_migrated": 0,
    "seeds_migrated": 0,
    "errors": 0,
}

def initialize_firebase():
    """Initialize Firebase Admin SDK."""
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_CREDENTIALS_PATH):
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
            print(f"✅ Firebase initialized from {FIREBASE_CREDENTIALS_PATH}")
        else:
            # Fallback to env var
            creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if creds_json:
                creds_dict = json.loads(creds_json)
                cred = credentials.Certificate(creds_dict)
                firebase_admin.initialize_app(cred)
            else:
                raise Exception("No Firebase credentials found")
    return firestore.client()

async def migrate_preferences(db):
    print("\n" + "="*60)
    print("MIGRATING PREFERENCES (Phase 5 V2)")
    print("="*60)
    
    users_ref = db.collection("users")
    users = users_ref.stream()
    
    async with httpx.AsyncClient() as client:
        # Prepare batch payloads
        genre_batch = []
        provider_batch = []
        seed_batch = []
        
        batch_size = 50 
        
        for user_doc in users:
            uid = user_doc.id
            data = user_doc.to_dict()
            prefs = data.get("preferences", {})
            stats["users_processed"] += 1
            
            # 1. Genres
            # Try 'selectedGenreIds' first (numeric), then 'selectedGenres' (string names - might map later but V2 plan implies IDs)
            # Actually, the V2 Schema uses genre_id INTEGER.
            # If Flutter sends IDs, we rely on 'selectedGenreIds'.
            genre_ids = prefs.get("selectedGenreIds", [])
            for gid in genre_ids:
                if isinstance(gid, int):
                    genre_batch.append({
                        "user_id": uid,
                        "genre_id": gid,
                        "weight": 1.0,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    })
            
            # 2. Providers
            # Expected format: List of objects with providerId
            providers = prefs.get("streamingProviders", [])
            for p in providers:
                # p can be a Dict
                if isinstance(p, dict):
                    pid = p.get("providerId")
                    pname = p.get("providerName", "Unknown")
                    logo = p.get("logoPath")
                    if pid:
                        provider_batch.append({
                            "user_id": uid,
                            "provider_id": int(pid),
                            "provider_name": pname,
                            "logo_path": logo,
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        })
            
            # 3. Seed Content (selectedMovies/Shows)
            # These go to user_titles
            selected_movies = prefs.get("selectedMovies", [])
            selected_shows = prefs.get("selectedShows", [])
            all_seeds = selected_movies + selected_shows
            
            for seed in all_seeds:
                if isinstance(seed, dict):
                    tid = seed.get("id") or seed.get("tmdbId")
                    if tid:
                        title_id = str(tid)
                        # Determine media type logic if missing? Usually present.
                        media_type = seed.get("mediaType", "movie") 
                        
                        seed_batch.append({
                            "user_id": uid,
                            "title_id": title_id,
                            "media_type": media_type,
                            "title": seed.get("title", ""),
                            "poster_path": seed.get("posterPath"),
                            "status": None,
                            "is_favorite": False,
                            "rating": None, # Implicit positive signal but no explicit rating
                            "source": "onboarding_seed",
                            "added_at": datetime.now(timezone.utc).isoformat(),
                            "synced_at": datetime.now(timezone.utc).isoformat(),
                        })

            # Flush batches if larger than batch_size
            if len(genre_batch) >= batch_size:
                await flush_batch(client, "user_genre_preferences", genre_batch)
                stats["genres_migrated"] += len(genre_batch)
                genre_batch = []
                
            if len(provider_batch) >= batch_size:
                await flush_batch(client, "user_provider_preferences", provider_batch)
                stats["providers_migrated"] += len(provider_batch)
                provider_batch = []
                
            if len(seed_batch) >= batch_size:
                await flush_batch(client, "user_titles", seed_batch)
                stats["seeds_migrated"] += len(seed_batch)
                seed_batch = []
                
            if stats["users_processed"] % 100 == 0:
                print(f"  Processed {stats['users_processed']} users...")

        # Flush remaining
        if genre_batch:
            await flush_batch(client, "user_genre_preferences", genre_batch)
            stats["genres_migrated"] += len(genre_batch)
            
        if provider_batch:
            await flush_batch(client, "user_provider_preferences", provider_batch)
            stats["providers_migrated"] += len(provider_batch)
            
        if seed_batch:
            await flush_batch(client, "user_titles", seed_batch)
            stats["seeds_migrated"] += len(seed_batch)

    print(f"\n✅ Users Processed: {stats['users_processed']}")
    print(f"✅ Genres Migrated: {stats['genres_migrated']}")
    print(f"✅ Providers Migrated: {stats['providers_migrated']}")
    print(f"✅ Seed Titles Migrated: {stats['seeds_migrated']}")


async def flush_batch(client, table, data):
    if not data:
        return
    try:
        response = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            json=data,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal"
            },
            timeout=10.0
        )
        if response.status_code not in [200, 201, 204]:
            print(f"Error flushing to {table}: {response.status_code} {response.text[:100]}")
            stats["errors"] += 1
    except Exception as e:
        print(f"Exception flushing to {table}: {e}")
        stats["errors"] += 1

async def main():
    if not SUPABASE_URL:
        print("❌ Missing Supabase Config")
        return
        
    db = initialize_firebase()
    await migrate_preferences(db)

if __name__ == "__main__":
    asyncio.run(main())
