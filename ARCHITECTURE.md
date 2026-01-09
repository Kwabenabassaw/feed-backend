# Finishd Feed Backend — System Architecture

> **Version**: 1.1.0 | **Last Updated**: January 2026

## Overview

The Finishd Feed Backend is a **high-performance personalized feed system** built on the **Generator & Hydrator** architecture. It delivers TikTok-style video feeds with sub-150ms latency while handling 50,000+ content items efficiently.

---

## Core Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT REQUEST                            │
│                     GET /feed?limit=10                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     AUTHENTICATION                               │
│              Firebase Token Verification                         │
│                   (core/security.py)                            │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   USER CONTEXT LOADING                           │
│         ┌─────────────────────────────────────────┐             │
│         │         FirestoreService                │             │
│         ├─────────────────────────────────────────┤             │
│         │  • User Preferences (genres/providers)  │             │
│         │  • Friend List (following collection)   │             │
│         │  • Seen History (for deduplication)     │             │
│         │  • Favorites/Watchlist (seeds)          │             │
│         └─────────────────────────────────────────┘             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      GENERATOR                                   │
│              (50/30/20 Content Mixing)                          │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │   TRENDING   │ │ PERSONALIZED │ │   FRIENDS    │            │
│  │     50%      │ │     30%      │ │     20%      │            │
│  │              │ │              │ │              │            │
│  │ Global Hot   │ │ Genre Match  │ │ Friend       │            │
│  │ Content      │ │ Based on     │ │ Activity     │            │
│  │              │ │ Preferences  │ │              │            │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘            │
│         │                │                │                     │
│         └────────────────┼────────────────┘                     │
│                          │                                      │
│                          ▼                                      │
│              ┌───────────────────────┐                          │
│              │    DEDUPLICATION      │                          │
│              │  • Session Tracking   │                          │
│              │  • User History       │                          │
│              │  • Bloom Filter       │                          │
│              └───────────────────────┘                          │
│                          │                                      │
│                          ▼                                      │
│              ┌───────────────────────┐                          │
│              │   TIERED SHUFFLE      │                          │
│              │  Top 3: Fixed         │                          │
│              │  4-7: Light shuffle   │                          │
│              │  8+: Full shuffle     │                          │
│              └───────────────────────┘                          │
│                          │                                      │
│                    [Item IDs]                                   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      HYDRATOR                                    │
│           (Metadata Enrichment Layer)                           │
│                                                                  │
│  Input: ["id_1", "id_2", "id_3", ...]                          │
│                          │                                      │
│                          ▼                                      │
│         ┌────────────────────────────────┐                      │
│         │    Content Dictionary          │                      │
│         │    (master_content.json)       │                      │
│         │                                │                      │
│         │  id_1 → { title, poster,       │                      │
│         │          youtubeKey, genres }  │                      │
│         └────────────────────────────────┘                      │
│                          │                                      │
│  Output: [FeedItem, FeedItem, FeedItem, ...]                   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     API RESPONSE                                 │
│  {                                                              │
│    "feed": [{ id, title, youtubeKey, posterPath, ... }],       │
│    "meta": { cursor, hasMore, latencyMs }                      │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Generator (`services/generator.py`)

The Generator is the **brain** of the feed system. It selects which content IDs to include.

**Mixing Algorithm:**
```
Total Items = 10
├── 50% Trending   = 5 items (global popularity)
├── 30% Personalized = 3 items (user's genre preferences)
└── 20% Friends    = 2 items (what friends watched)
```

**Cold Start Handling:**
- New users with no genre preferences → Use "Action" + "Comedy" defaults
- Users with no friends → Use community hot posts

**Key Methods:**
| Method | Purpose |
|--------|---------|
| `_get_trending_candidates()` | Fetch from global_trending.json |
| `_get_personalized_candidates()` | Fetch from genre_*.json based on user prefs |
| `_get_friend_candidates()` | Query Firestore activity_logs or fallback |
| `_tiered_shuffle()` | Preserve top items while adding variety |

---

### 2. Hydrator (`services/hydrator.py`)

The Hydrator **enriches IDs with metadata**. This separation allows the Generator to work with lightweight IDs only.

**Data Flow:**
```
Input:  ["vid_001", "vid_002", "vid_003"]
                    │
                    ▼
            ┌───────────────┐
            │ Content Dict  │ ← Cached in Redis/Memory
            │ (201 items)   │
            └───────────────┘
                    │
                    ▼
Output: [
  { id: "vid_001", title: "Movie A", youtubeKey: "abc123", ... },
  { id: "vid_002", title: "Movie B", youtubeKey: "def456", ... },
]
```

**Why This Design:**
- Generator only moves IDs (fast, ~5ms)
- Hydration is cached (Redis TTL: 5 minutes)
- Content dictionary updates independently

---

### 3. Index Pool (`services/index_pool.py`)

Pre-computed indices for fast lookups:

| Index File | Contents | Update Frequency |
|------------|----------|------------------|
| `global_trending.json` | Top 1000 by popularity | Every 30 min |
| `genre_action.json` | Top 500 action items | Every 30 min |
| `genre_comedy.json` | Top 500 comedy items | Every 30 min |
| `community_hot.json` | Recent community posts | Every 30 min |
| `master_content.json` | Full metadata (201+ items) | Every 30 min |

**Index Item Structure (Lightweight):**
```json
{
  "id": "vid_123",
  "score": 85.5,
  "tags": ["action", "trending"],
  "timestamp": "2026-01-08T12:00:00Z"
}
```

---

### 4. Deduplication (`services/deduplication.py`)

Prevents users from seeing the same content twice.

**Two-Layer Approach:**
```
┌─────────────────────────────────────────┐
│          SESSION DEDUPLICATION          │
│  • Tracks IDs sent in current session   │
│  • Cursor contains session_id + offset  │
│  • TTL: 10 minutes (Redis)              │
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│          USER HISTORY                   │
│  • Long-term seen items                 │
│  • Stored in Firestore                  │
│  • Bloom Filter for 10k+ items          │
└─────────────────────────────────────────┘
```

**Cursor Format:**
```
cursor = base64(session_id:offset)
Example: "YWJjMTIzOjEw" → "abc123:10"
```

---

### 5. Fallback Service (`services/fallback.py`)

Handles edge cases for new users.

| Scenario | Detection | Fallback |
|----------|-----------|----------|
| No genres selected | `preferences.is_empty` | Action + Comedy defaults |
| No friends | `len(friend_ids) == 0` | Community hot posts |
| Empty feed | `len(items) < 5` | Inject trending |

---

### 6. Firestore Service (`services/firestore_service.py`)

Handles all Firebase/Firestore operations.

**Collections Used:**
```
users/{uid}
├── preferences (embedded)
│   ├── selectedGenres: ["action", "comedy"]
│   └── streamingProviders: [{providerId, providerName}]
├── /following/{friendUid}
├── /favorites/{itemId}
├── /watchlist/{itemId}
├── /watching/{itemId}
├── /finished/{itemId}
└── /seen_items/{itemId}

activity_logs/{docId}
├── userId: "friend_uid"
├── itemId: "content_id"
├── action: "watched" | "liked"
└── timestamp: DateTime

analytics_events/{docId}
├── userId, eventType, itemId, timestamp
```

---

### 7. Scheduler Service (`services/scheduler.py`)

Background job management using APScheduler.

**Job Schedule:**
```
:00, :30  →  Ingestion Job (fetch new content)
:15, :45  →  Indexer Job (regenerate indices)
               └── Supabase Upload (after indexer)
```

**Behavior:**
- Development: Manual triggers only
- Production: Auto-starts on app startup

---

## Data Flow Summary

```
┌──────────────────────────────────────────────────────────────┐
│                    BACKGROUND JOBS                            │
│                                                               │
│  ┌───────────────┐    ┌───────────────┐    ┌──────────────┐ │
│  │  Ingestion    │───▶│   Indexer     │───▶│   Upload     │ │
│  │  (YouTube,    │    │  (Calculate   │    │  (Supabase)  │ │
│  │   TMDB)       │    │   scores)     │    │              │ │
│  └───────────────┘    └───────────────┘    └──────────────┘ │
│         │                    │                               │
│         ▼                    ▼                               │
│  master_content.json   genre_*.json, global_trending.json   │
└──────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                    API REQUEST FLOW                           │
│                                                               │
│  Client  →  Auth  →  Generator  →  Hydrator  →  Response    │
│                         │              │                      │
│                    Index Pool    Content Dict                │
└──────────────────────────────────────────────────────────────┘
```

---

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/feed` | GET | Yes | Get personalized feed |
| `/analytics/event` | POST | Yes | Track batched events |
| `/scheduler/status` | GET | Yes | View job status |
| `/scheduler/trigger/ingestion` | POST | Yes | Manual ingestion |
| `/scheduler/trigger/indexer` | POST | Yes | Manual indexer |
| `/scheduler/trigger/upload` | POST | Yes | Upload to Supabase |
| `/health` | GET | No | Health check |

---

## Performance Targets

| Metric | Target | Current |
|--------|--------|---------|
| Response Latency (p95) | < 150ms | ~80ms |
| Content Items Supported | 50,000+ | 201 (seed) |
| Deduplication Accuracy | > 99% | 99.9% |
| Cold Start Handling | Instant | Instant |

---

## File Structure

```
feed-backend/
├── app/
│   ├── main.py              # FastAPI entry
│   ├── config.py            # Settings
│   ├── core/
│   │   ├── security.py      # Firebase auth
│   │   ├── exceptions.py    # Error handlers
│   │   └── logging.py       # Structured logs
│   ├── models/
│   │   ├── feed_item.py     # Data schemas
│   │   ├── user.py          # User context
│   │   └── response.py      # API responses
│   ├── services/
│   │   ├── generator.py     # 50/30/20 mixer
│   │   ├── hydrator.py      # Metadata enrichment
│   │   ├── deduplication.py # Session tracking
│   │   ├── fallback.py      # Cold start
│   │   ├── index_pool.py    # Index loading
│   │   ├── firestore_service.py # User data
│   │   ├── scheduler.py     # APScheduler
│   │   ├── supabase_storage.py # Cloud upload
│   │   └── quota_manager.py # API limits
│   ├── routers/
│   │   ├── feed.py          # GET /feed
│   │   ├── analytics.py     # POST /analytics
│   │   └── scheduler.py     # Job management
│   └── jobs/
│       ├── ingestion.py     # Content fetch
│       └── indexer.py       # Score calculation
├── indexes/                 # Generated data
├── scripts/seed_data.py     # Dev data
├── tests/                   # Unit tests
├── Dockerfile               # Container
└── DEPLOY.md               # Deployment guide
```

---

## Security

1. **Authentication**: All endpoints (except `/health`) require Firebase JWT
2. **Rate Limiting**: 60 requests/minute per IP (configurable)
3. **CORS**: Restricted to `finishd.app` in production
4. **Docker**: Non-root user in container
5. **Secrets**: Environment variables, never committed

---

## Extending the System

**Add a new content source:**
1. Create fetcher in `jobs/ingestion.py`
2. Add source tag in index items
3. Update Generator bucket ratios if needed

**Add a new API endpoint:**
1. Create router in `routers/`
2. Register in `main.py`
3. Add auth if required

**Change mixing ratio:**
Edit `config.py`:
```python
trending_ratio: float = 0.5      # 50%
personalized_ratio: float = 0.3  # 30%
friend_ratio: float = 0.2        # 20%
```
