"""
Firebase to Supabase Migration Script

Migrates existing user profiles and follow relationships from Firebase to Supabase.
Run this script once to backfill existing data.

Usage:
    cd feed-backend
    python scripts/migrate_firebase_to_supabase.py

Requirements:
    - Firebase service account credentials configured
    - Supabase URL and service key in environment
"""

import os
import sys
import json
import asyncio
from typing import List, Dict, Any

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
    "profiles_migrated": 0,
    "profiles_failed": 0,
    "follows_migrated": 0,
    "follows_failed": 0,
}


def initialize_firebase():
    """Initialize Firebase Admin SDK."""
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_CREDENTIALS_PATH):
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
            print(f"✅ Firebase initialized from {FIREBASE_CREDENTIALS_PATH}")
        else:
            # Try environment variable
            creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if creds_json:
                creds_dict = json.loads(creds_json)
                cred = credentials.Certificate(creds_dict)
                firebase_admin.initialize_app(cred)
                print("✅ Firebase initialized from environment variable")
            else:
                raise Exception("No Firebase credentials found")
    return firestore.client()


async def migrate_profiles(db, batch_size: int = 100):
    """Migrate all user profiles from Firebase to Supabase."""
    print("\n" + "="*60)
    print("MIGRATING PROFILES")
    print("="*60)
    
    users_ref = db.collection("users")
    docs = users_ref.stream()
    
    batch = []
    async with httpx.AsyncClient() as client:
        for doc in docs:
            data = doc.to_dict()
            uid = doc.id
            
            profile = {
                "id": uid,
                "email": data.get("email", ""),
                "username": data.get("username", ""),
                "profile_image": data.get("profileImage", ""),
            }
            batch.append(profile)
            
            if len(batch) >= batch_size:
                await upsert_profiles_batch(client, batch)
                batch = []
        
        # Handle remaining
        if batch:
            await upsert_profiles_batch(client, batch)
    
    print(f"\n✅ Profiles migrated: {stats['profiles_migrated']}")
    print(f"❌ Profiles failed: {stats['profiles_failed']}")


async def upsert_profiles_batch(client: httpx.AsyncClient, profiles: List[Dict]):
    """Upsert a batch of profiles to Supabase."""
    try:
        response = await client.post(
            f"{SUPABASE_URL}/rest/v1/profiles",
            json=profiles,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            timeout=30.0,
        )
        
        if response.status_code in [200, 201]:
            stats["profiles_migrated"] += len(profiles)
            print(f"  ✓ Migrated {len(profiles)} profiles")
        else:
            stats["profiles_failed"] += len(profiles)
            print(f"  ✗ Failed batch: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        stats["profiles_failed"] += len(profiles)
        print(f"  ✗ Error: {e}")


async def migrate_follows(db, batch_size: int = 100):
    """Migrate all follow relationships from Firebase to Supabase."""
    print("\n" + "="*60)
    print("MIGRATING FOLLOWS")
    print("="*60)
    
    users_ref = db.collection("users")
    docs = users_ref.stream()
    
    batch = []
    async with httpx.AsyncClient() as client:
        for user_doc in docs:
            uid = user_doc.id
            
            # Get following subcollection
            following_ref = users_ref.document(uid).collection("following")
            following_docs = following_ref.stream()
            
            for follow_doc in following_docs:
                target_uid = follow_doc.id
                follow_data = follow_doc.to_dict()
                
                follow = {
                    "follower_id": uid,
                    "following_id": target_uid,
                }
                batch.append(follow)
                
                if len(batch) >= batch_size:
                    await upsert_follows_batch(client, batch)
                    batch = []
        
        # Handle remaining
        if batch:
            await upsert_follows_batch(client, batch)
    
    print(f"\n✅ Follows migrated: {stats['follows_migrated']}")
    print(f"❌ Follows failed: {stats['follows_failed']}")


async def upsert_follows_batch(client: httpx.AsyncClient, follows: List[Dict]):
    """Upsert a batch of follows to Supabase."""
    try:
        response = await client.post(
            f"{SUPABASE_URL}/rest/v1/follows",
            json=follows,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=ignore-duplicates,return=minimal",
            },
            timeout=30.0,
        )
        
        if response.status_code in [200, 201]:
            stats["follows_migrated"] += len(follows)
            print(f"  ✓ Migrated {len(follows)} follows")
        else:
            stats["follows_failed"] += len(follows)
            print(f"  ✗ Failed batch: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        stats["follows_failed"] += len(follows)
        print(f"  ✗ Error: {e}")


async def main():
    """Main migration function."""
    print("\n" + "="*60)
    print("FIREBASE → SUPABASE MIGRATION")
    print("="*60)
    
    # Validate config
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        sys.exit(1)
    
    print(f"Supabase URL: {SUPABASE_URL}")
    
    # Initialize Firebase
    db = initialize_firebase()
    
    # Step 1: Migrate profiles first (needed for FK constraints)
    await migrate_profiles(db)
    
    # Step 2: Migrate follows (depends on profiles existing)
    await migrate_follows(db)
    
    # Summary
    print("\n" + "="*60)
    print("MIGRATION COMPLETE")
    print("="*60)
    print(f"Profiles: {stats['profiles_migrated']} migrated, {stats['profiles_failed']} failed")
    print(f"Follows:  {stats['follows_migrated']} migrated, {stats['follows_failed']} failed")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
