
import os
import sys
import asyncio
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from typing import List, Dict, Any
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
FIREBASE_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON") or "firebase_credentials.json"

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("❌ Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
    sys.exit(1)

# Initialize Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
    firebase_admin.initialize_app(cred)

db = firestore.client()

async def migrate_social_graph():
    print(f"🚀 Starting Social Graph Migration to {SUPABASE_URL}")
    print(f"📅 Time: {datetime.now()}")

    stats = {
        "users_scanned": 0,
        "relationships_found": 0,
        "synced": 0,
        "errors": 0,
        "skipped": 0
    }

    # Batch config
    BATCH_SIZE = 100
    batch_records = []

    try:
        # 1. Fetch all users
        users_ref = db.collection("users")
        users = users_ref.stream()

        async with httpx.AsyncClient() as client:
            for user_doc in users:
                uid = user_doc.id
                stats["users_scanned"] += 1
                
                if stats["users_scanned"] % 100 == 0:
                    print(f"Scanning user #{stats['users_scanned']} ({uid})...")

                # 2. Fetch 'following' subcollection
                following_ref = users_ref.document(uid).collection("following")
                following_docs = following_ref.stream()

                for follow_doc in following_docs:
                    target_uid = follow_doc.id
                    data = follow_doc.to_dict()
                    
                    # Some following docs rely on 'followedAt', others might just be empty docs
                    # We default created_at to now if missing, or use followedAt if available
                    created_at = datetime.now().isoformat()
                    if "followedAt" in data and data["followedAt"]:
                        # Convert Firestore timestamp to ISO
                        dt = data["followedAt"]
                        if hasattr(dt, "isoformat"):
                            created_at = dt.isoformat()

                    record = {
                        "follower_id": uid,
                        "following_id": target_uid,
                        "created_at": created_at
                    }
                    
                    batch_records.append(record)
                    stats["relationships_found"] += 1

                    # 3. Batch UPSERT to Supabase
                    if len(batch_records) >= BATCH_SIZE:
                        await flush_batch(client, batch_records, stats)
                        batch_records = []

            # Flush remaining
            if batch_records:
                await flush_batch(client, batch_records, stats)

    except Exception as e:
        print(f"\n❌ Fatal Error: {str(e)}")
        import traceback
        traceback.print_exc()
        stats["errors"] += 1

    print("\n" + "="*50)
    print("MIGRATION COMPLETE")
    print("="*50)
    print(f"Users Scanned:      {stats['users_scanned']}")
    print(f"Relationships:      {stats['relationships_found']}")
    print(f"Synced Successfully:{stats['synced']}")
    print(f"Errors:             {stats['errors']}")
    print("="*50)

async def flush_batch(client, records: List[Dict], stats: Dict):
    if not records:
        return

    try:
        response = await client.post(
            f"{SUPABASE_URL}/rest/v1/follows",
            json=records,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                # Ignore duplicates to be safe (idempotent)
                "Prefer": "resolution=ignore-duplicates,return=minimal",
            },
            timeout=30.0
        )

        if response.status_code in [200, 201]:
            stats["synced"] += len(records)
            print(f"✅ Synced batch of {len(records)} relationships")
        elif response.status_code == 409:
             # Conflict means FK violation (user doesn't exist in profiles yet)
             # We can try to insert one by one or just log warning?
             # For social graph, strict integrity is key.
             # But if profiles migrated first, this should be fine.
             print(f"⚠️ Batch conflict (referential integrity?): {response.text[:100]}")
             # Retry logic could go here, but for now we accept loss or manual fix
             stats["errors"] += len(records)
        else:
            print(f"❌ Batch error {response.status_code}: {response.text[:100]}")
            stats["errors"] += len(records)

    except Exception as e:
        print(f"❌ Batch exception: {str(e)}")
        stats["errors"] += len(records)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(migrate_social_graph())
