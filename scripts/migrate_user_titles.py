"""
User Titles Migration Script (Phase 3 - Corrected)

Migrates existing user watchlist/watching/finished/favorites data from Firebase
to the normalized user_titles table in Supabase.

Corrections Applied:
1. CORRECTION #2: added_at computed as earliest timestamp, never NULL
2. CORRECTION #6: Conflict resolution precedence: finished > watching > watchlist
3. One row per (user_id, title_id)
4. Ratings from user_titles collection override list ratings
5. Favorites merged independently

Usage:
    cd feed-backend
    python scripts/migrate_user_titles.py

Requirements:
    - Firebase service account credentials configured
    - Supabase URL and service key in environment
    - Phase 3 migration (003_create_user_titles_table.sql) already applied
"""

import os
import sys
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

# Add parent directory to path for imports
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

# Stats
stats = {
    "users_processed": 0,
    "titles_migrated": 0,
    "titles_failed": 0,
    "ratings_merged": 0,
}

# Status precedence (CORRECTION #6)
STATUS_PRECEDENCE = ["finished", "watching", "watchlist"]


def initialize_firebase():
    """Initialize Firebase Admin SDK."""
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_CREDENTIALS_PATH):
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
            print(f"✅ Firebase initialized from {FIREBASE_CREDENTIALS_PATH}")
        else:
            creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if creds_json:
                creds_dict = json.loads(creds_json)
                cred = credentials.Certificate(creds_dict)
                firebase_admin.initialize_app(cred)
                print("✅ Firebase initialized from environment variable")
            else:
                raise Exception("No Firebase credentials found")
    return firestore.client()


def get_earliest_timestamp(*timestamps) -> Optional[datetime]:
    """
    CORRECTION #2: Get earliest non-null timestamp.
    Returns None only if all timestamps are None.
    """
    valid = [t for t in timestamps if t is not None]
    if not valid:
        return None
    return min(valid)


def resolve_status(title_data: Dict[str, Any]) -> Optional[str]:
    """
    CORRECTION #6: Resolve status with precedence: finished > watching > watchlist
    """
    for status in STATUS_PRECEDENCE:
        if title_data.get(status):
            return status
    return None


async def migrate_user_titles(db, batch_size: int = 50):
    """
    Migrate all user titles from Firebase to Supabase.
    
    For each user:
    1. Read all 4 lists (watching, watchlist, finished, favorites)
    2. Read user_titles collection for ratings
    3. Merge into single record per title
    4. UPSERT to Supabase
    """
    print("\n" + "="*60)
    print("MIGRATING USER TITLES (Phase 3 - Corrected)")
    print("="*60)
    
    users_ref = db.collection("users")
    users = users_ref.stream()
    
    async with httpx.AsyncClient() as client:
        for user_doc in users:
            uid = user_doc.id
            stats["users_processed"] += 1
            
            try:
                # Step 1: Collect all title data from all lists
                title_map: Dict[str, Dict[str, Any]] = {}
                
                for list_name in ["watching", "watchlist", "finished", "favorites"]:
                    list_ref = users_ref.document(uid).collection(list_name)
                    list_docs = list_ref.stream()
                    
                    for doc in list_docs:
                        title_id = doc.id
                        data = doc.to_dict()
                        
                        if title_id not in title_map:
                            title_map[title_id] = {
                                "title_id": title_id,
                                "media_type": data.get("mediaType", "movie"),
                                "title": data.get("title", ""),
                                "poster_path": data.get("posterPath"),
                                "watching": False,
                                "watchlist": False,
                                "finished": False,
                                "is_favorite": False,
                                "rating": None,
                                "timestamps": [],
                                "favorited_at": None,
                            }
                        
                        # Mark which list(s) this title is in
                        if list_name == "favorites":
                            title_map[title_id]["is_favorite"] = True
                            added_at = data.get("addedAt")
                            if added_at:
                                title_map[title_id]["favorited_at"] = added_at.isoformat() if hasattr(added_at, 'isoformat') else str(added_at)
                        else:
                            title_map[title_id][list_name] = True
                        
                        # Collect rating from list (may be overridden by user_titles)
                        if data.get("rating"):
                            title_map[title_id]["rating"] = data.get("rating")
                        
                        # Collect timestamp for earliest calculation
                        added_at = data.get("addedAt")
                        if added_at:
                            if hasattr(added_at, 'timestamp'):
                                title_map[title_id]["timestamps"].append(added_at)
                
                # Step 2: Read user_titles for ratings (override list ratings)
                user_titles_ref = users_ref.document(uid).collection("user_titles")
                user_titles_docs = user_titles_ref.stream()
                
                for doc in user_titles_docs:
                    title_id = doc.id
                    data = doc.to_dict()
                    
                    if title_id in title_map:
                        # Override rating from user_titles
                        if data.get("rating"):
                            title_map[title_id]["rating"] = data.get("rating")
                            stats["ratings_merged"] += 1
                        
                        # Add ratedAt to timestamps
                        rated_at = data.get("ratedAt")
                        if rated_at and hasattr(rated_at, 'timestamp'):
                            title_map[title_id]["timestamps"].append(rated_at)
                
                # Step 3: Build Supabase records
                records = []
                for title_id, data in title_map.items():
                    # CORRECTION #6: Resolve status with precedence
                    status = resolve_status(data)
                    
                    # CORRECTION #2: Compute earliest timestamp
                    timestamps = data.get("timestamps", [])
                    if timestamps:
                        earliest = min(timestamps)
                        added_at = earliest.isoformat() if hasattr(earliest, 'isoformat') else str(earliest)
                    else:
                        # Fallback to NOW() if no timestamps (shouldn't happen)
                        added_at = datetime.now(timezone.utc).isoformat()
                    
                    # CORRECTION #3: status_changed_at is NULL when status is NULL
                    status_changed_at = added_at if status else None
                    
                    record = {
                        "user_id": uid,
                        "title_id": title_id,
                        "media_type": data.get("media_type", "movie"),
                        "title": data.get("title", ""),
                        "poster_path": data.get("poster_path"),
                        "status": status,
                        "is_favorite": data.get("is_favorite", False),
                        "rating": data.get("rating"),
                        "source": None,  # Unknown for migrated data
                        "added_at": added_at,
                        "status_changed_at": status_changed_at,
                        "rated_at": added_at if data.get("rating") else None,
                        "favorited_at": data.get("favorited_at"),
                        "synced_at": datetime.now(timezone.utc).isoformat(),
                    }
                    records.append(record)
                
                # Step 4: Batch UPSERT to Supabase
                if records:
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        await upsert_user_titles_batch(client, batch)
                
                if stats["users_processed"] % 10 == 0:
                    print(f"  Processed {stats['users_processed']} users...")
                    
            except Exception as e:
                print(f"  ✗ Error processing user {uid}: {e}")
    
    print(f"\n✅ Users processed: {stats['users_processed']}")
    print(f"✅ Titles migrated: {stats['titles_migrated']}")
    print(f"❌ Titles failed: {stats['titles_failed']}")
    print(f"📊 Ratings merged from user_titles: {stats['ratings_merged']}")


async def upsert_user_titles_batch(client: httpx.AsyncClient, records: List[Dict]):
    """UPSERT a batch of user_titles to Supabase."""
    try:
        response = await client.post(
            f"{SUPABASE_URL}/rest/v1/user_titles",
            json=records,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                # UPSERT: merge on conflict, preserve existing added_at
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            timeout=30.0,
        )
        
        if response.status_code in [200, 201]:
            stats["titles_migrated"] += len(records)
        else:
            stats["titles_failed"] += len(records)
            print(f"  ✗ Failed batch: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        stats["titles_failed"] += len(records)
        print(f"  ✗ Error: {e}")


async def main():
    """Main migration function."""
    print("\n" + "="*60)
    print("FIREBASE → SUPABASE USER TITLES MIGRATION")
    print("Phase 3 (Corrected)")
    print("="*60)
    
    # Validate config
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        sys.exit(1)
    
    print(f"Supabase URL: {SUPABASE_URL}")
    
    # Initialize Firebase
    db = initialize_firebase()
    
    # Migrate user titles
    await migrate_user_titles(db)
    
    # Summary
    print("\n" + "="*60)
    print("MIGRATION COMPLETE")
    print("="*60)
    print(f"Users:   {stats['users_processed']} processed")
    print(f"Titles:  {stats['titles_migrated']} migrated, {stats['titles_failed']} failed")
    print(f"Ratings: {stats['ratings_merged']} merged from user_titles")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
