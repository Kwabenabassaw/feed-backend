# Social Graph Sync - Architecture & Usage Guide

## Overview

The Social Graph Sync layer enables Finishd to maintain a **derived social graph in Supabase** that is synchronized from the **authoritative Firebase source**. This powers friend-based feed ranking, recommendations, and ML features.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Flutter Client                               │
│         (Firebase Auth + Firestore for realtime UX)            │
└─────────────────────────┬───────────────────────────────────────┘
                          │
              1. Follow/Unfollow
              2. Writes to Firebase
              3. Calls sync endpoint
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FastAPI Backend                              │
│              POST /social/follow                                 │
│                                                                  │
│  • Verifies Firebase ID token (extracts uid)                   │
│  • NEVER trusts client-sent uid                                │
│  • Uses Supabase service role (server-side only)               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Supabase Postgres                            │
│              follows + user_stats tables                         │
│                                                                  │
│  • Derived data (not source of truth)                          │
│  • Triggers maintain counts automatically                       │
│  • Powers analytics and ML queries                             │
└─────────────────────────────────────────────────────────────────┘
```

## Why Firebase is Authoritative

1. **Realtime UX**: Firebase subcollections provide instant follow/unfollow feedback
2. **Offline support**: Firestore handles offline writes gracefully
3. **Existing infrastructure**: App already uses Firebase for all user data
4. **Security rules**: Firestore security rules prevent unauthorized follows

## Why Supabase is Derived

1. **SQL power**: Complex graph queries (mutuals, friend-of-friends) are easy in SQL
2. **Analytics**: Aggregate stats, trending friends, recommendation algorithms
3. **ML features**: Clean relational data for model training
4. **No realtime requirement**: Intelligence queries are not latency-sensitive

## Endpoints

### POST /social/follow

Sync a follow/unfollow action from Firebase to Supabase.

**Request:**
```bash
curl -X POST https://api.finishd.app/social/follow \
  -H "Authorization: Bearer <FIREBASE_ID_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "target_uid": "firebase_uid_of_target_user",
    "action": "follow"
  }'
```

**Response:**
```json
{
  "success": true,
  "action": "follow",
  "follower_uid": "authenticated_user_uid",
  "target_uid": "target_user_uid",
  "message": "Follow synced successfully"
}
```

### GET /social/followers/{user_id}

Get followers of a user (for analytics).

```bash
curl https://api.finishd.app/social/followers/user_123 \
  -H "Authorization: Bearer <FIREBASE_ID_TOKEN>"
```

### GET /social/following/{user_id}

Get users that a user follows.

### GET /social/stats/{user_id}

Get cached follower/following counts.

### GET /social/mutuals/{user_id}

Get mutual follows (users who follow each other).

## Idempotency & Retry Safety

| Action | SQL Operation | Retry Behavior |
|--------|--------------|----------------|
| Follow | `UPSERT (ignore duplicates)` | Safe to retry - no duplicate rows |
| Unfollow | `DELETE WHERE` | Safe to retry - no error if missing |

## Security Model

1. **Token verification**: Firebase Admin SDK verifies ID token
2. **UID extraction**: User ID comes from verified token only
3. **Service role**: Supabase credentials never exposed to client
4. **Self-follow prevention**: Endpoint rejects `follower_id == following_id`

## Database Schema

### follows table

```sql
CREATE TABLE follows (
    follower_id TEXT NOT NULL,    -- Firebase UID
    following_id TEXT NOT NULL,   -- Firebase UID
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (follower_id, following_id),
    CONSTRAINT no_self_follow CHECK (follower_id != following_id)
);
```

### user_stats table

```sql
CREATE TABLE user_stats (
    user_id TEXT PRIMARY KEY,
    followers_count INTEGER DEFAULT 0,
    following_count INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

Triggers automatically update counts on INSERT/DELETE.

## Environment Variables

```bash
# Required for Supabase sync
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key

# Required for Firebase token verification
GOOGLE_APPLICATION_CREDENTIALS_JSON={"type":"service_account",...}
```

## Error Handling

| Error | HTTP Code | Cause |
|-------|-----------|-------|
| Missing token | 401 | No Authorization header |
| Invalid token | 401 | Firebase token verification failed |
| Self-follow | 400 | target_uid == authenticated uid |
| Supabase error | 500 | Database operation failed |
| Timeout | 504 | Supabase didn't respond in 10s |

## Flutter Integration

The Flutter client should call the sync endpoint **after** the Firebase write succeeds:

```dart
// In user_service.dart - after Firebase batch commit
Future<void> followUser(String currentUid, String targetUid) async {
  // ... existing Firebase batch write ...
  await batch.commit();
  
  // Sync to Supabase (fire and forget, or await for confirmation)
  try {
    await _syncFollowToSupabase(targetUid, 'follow');
  } catch (e) {
    // Log but don't fail - Firebase is source of truth
    debugPrint('Supabase sync failed: $e');
  }
}

Future<void> _syncFollowToSupabase(String targetUid, String action) async {
  final token = await FirebaseAuth.instance.currentUser?.getIdToken();
  await http.post(
    Uri.parse('$backendUrl/social/follow'),
    headers: {
      'Authorization': 'Bearer $token',
      'Content-Type': 'application/json',
    },
    body: jsonEncode({
      'target_uid': targetUid,
      'action': action,
    }),
  );
}
```
