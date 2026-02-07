# Audit Report: Finishd Backend

## Section 1: 🚨 Critical Backend Risks

### 1. Friend Activity Read Amplification (O(N) Fan-Out on Read)
*   **Issue:** The `_get_friend_candidates` method in `Generator` triggers a massive read operation. It calls `firestore.get_friend_activity`, which iterates through a user's friend list in chunks of 30, executing a Firestore query for each chunk.
*   **Why it will break at scale:** If a user follows 500 people, this results in ~17 sequential Firestore queries *every time* they load the feed. This is O(N) where N is the number of friends. It will cause high latency and explode Firestore read costs.
*   **Concrete Fix:** Switch to **Fan-Out on Write**. When a user performs an activity (watch/like), write that event to their followers' "timeline" collections (or a simplified `feed_fanout` collection). Then, the feed generator only reads from the user's own timeline (O(1) read).
*   **Affected Service:** `app/services/generator.py`, `app/services/firestore_service.py`

### 2. Non-Deterministic Feed Ranking
*   **Issue:** `_tiered_shuffle` uses Python's `random.shuffle` without a seed.
*   **Why it will break at scale:**
    *   **UX:** Refreshing the feed (without changing parameters) changes the order, violating the "Stable across pagination" requirement.
    *   **Pagination:** Since the shuffle is random per request, an item at index 10 on Page 1 might appear at index 12 on Page 2 (if re-fetched/re-shuffled), causing duplicates or missing items.
*   **Concrete Fix:** Seed the random number generator with a combination of `session_id` and `page_offset` (or a stable request ID) to ensure the shuffle is deterministic for the same session/page.
*   **Affected Service:** `app/services/generator.py`

### 3. Unstable "Seen-State" Pagination
*   **Issue:** The pagination strategy fetches the *top N* candidates from indices (Trending/Genre) and filters out items in `session_seen`.
*   **Why it will break at scale:** This relies on the underlying index (e.g., `global_trending.json`) being stable. If the index updates (every 30 mins) while a user is paginating, the "top N" shifts. Items that move up might be seen again (if not perfectly tracked), and items that move down might be skipped entirely.
*   **Concrete Fix:**
    *   **Option A:** Generate a static "Feed Plan" (list of IDs) for the session stored in Redis. Pagination just slices this list.
    *   **Option B:** Use a cursor that encodes the *score* of the last seen item, not just an offset, and query for items with `score < last_score`.
*   **Affected Service:** `app/services/generator.py`

### 4. Local Memory Fallback for Session State
*   **Issue:** `DeduplicationService` falls back to `_local_sessions` (in-memory dict) if Redis is unavailable.
*   **Why it will break at scale:** In a production environment with multiple worker processes (or pods), local memory is not shared. A user's Page 1 request might hit Worker A, and Page 2 might hit Worker B. Worker B won't know what was seen in Page 1, leading to duplicates.
*   **Concrete Fix:** Remove the local memory fallback for production. The system should fail fast or use a reliable distributed cache (Redis/Memcached).
*   **Affected Service:** `app/services/deduplication.py`

---

## Section 2: ⚠️ Performance & Cost Issues

### 1. Hydrator Loads Entire Content Dictionary into Memory
*   **Issue:** `Hydrator._load_content_dictionary` fetches `master_content.json` (the entire content library) and loads it into a Python dictionary in memory.
*   **Why it's a problem:**
    *   **Memory:** As content grows to 50k+ items, this JSON object will consume significant RAM per worker.
    *   **Latency:** Deserializing a massive JSON blob is CPU intensive and blocks the event loop.
*   **Concrete Fix:** Store content metadata in Redis as individual keys (e.g., `content:{id}`) or use `HGET` commands. Fetch only the specific IDs needed for the page (`MGET` or `HMGET`).
*   **Affected Service:** `app/services/hydrator.py`

### 2. Firestore Read Multipliers in User Context
*   **Issue:** `load_user_context` performs 5 parallel Firestore reads (Preferences, Friends, Seen Items, Favorites, Watchlist) on every cache miss.
*   **Why it's a problem:**
    *   **Cost:** High read volume per active user session.
    *   **Latency:** Fan-out queries increase the p99 latency risk.
*   **Concrete Fix:**
    *   **Optimize:** Store a "lite" user profile in Redis that is updated only when changed (write-through cache).
    *   **Denormalize:** Store `selectedGenres` and `friendIds` directly on the User object in Firestore to reduce 2 reads to 1.
*   **Affected Service:** `app/services/firestore_service.py`

### 3. Index Loading Scalability
*   **Issue:** `IndexPoolService` loads entire genre JSON files into memory.
*   **Why it's a problem:** Similar to the Hydrator, as the number of items per genre grows, loading and parsing these files becomes a bottleneck. `sorted(items, key=...)` is performed on the full list every time `get_trending_ids` is called, which is CPU intensive for large N.
*   **Concrete Fix:** Use a sorted set in Redis (`ZSET`) for ranking. `ZRANGE` is O(log(N) + M), much faster than sorting in Python.
*   **Affected Service:** `app/services/index_pool.py`

---

## Section 3: 🧱 Architectural Violations

| Original Decision Violated | Current Behavior | Correct Alignment |
|----------------------------|------------------|-------------------|
| **Feed ranking is deterministic per request** | `_tiered_shuffle` uses `random.shuffle()` which is non-deterministic and seedless. | Use a PRNG seeded with `session_id + offset` to guarantee stable shuffling for the duration of the session. |
| **Firestore used for ranked feeds** | Friend activity query iterates through Firestore collections, treating it like a relational DB join. | Denormalize activity into a single "timeline" collection per user (Fan-out on Write) or use a purpose-built feed engine. |
| **Hydrator remains lightweight** | Hydrator loads the entire world (all 50k+ items) into memory to hydrate just 10 items. | Hydrator should act as a key-value look-up service, fetching only the requested IDs from Redis or Firestore. |
| **Pagination guarantees stability** | Pagination filters "seen" items from a live, shifting index, causing instability. | Use "Feed Plans" (snapshot of IDs in Redis) or cursor-based pagination on stable sorts (score/time). |

---

## Section 4: 🧠 Ideal Backend Feed Architecture

### Generator Responsibilities
*   **Session Planning:** Generate a stable list of Candidate IDs ("Feed Plan") at the start of a session and store in Redis (TTL 10m).
*   **Slicing:** Pagination requests simply retrieve the slice `[offset : offset + limit]` from this pre-computed plan.
*   **Mixing:** Apply the 50/30/20 mix logic *once* during plan generation, not per page.

### Ranker Responsibilities
*   **Scoring:** Score candidates based on relevance signals (recency, popularity, user affinity) *before* they enter the Feed Plan.
*   **Separation:** Ranking logic should be decoupled from the data fetching logic.

### Hydrator Responsibilities
*   **Precision Fetching:** Accept a list of IDs and return metadata.
*   **Efficient Storage:** Use Redis `MGET` (or Firestore `getAll`) to fetch *only* the requested items. Never load the full dictionary.
*   **Caching:** Cache individual item metadata, not the whole library.

### Analytics Pipeline
*   **Async Ingestion:** Analytics endpoints write to a message queue (Pub/Sub, Kafka) or high-throughput stream.
*   **Background Processing:** Workers consume the stream to update aggregates (views, likes) and user history in batch.
*   **Decoupling:** Feed generation should not block on analytics writes (currently `save_analytics_events` is async but still hits Firestore directly).

---

## Section 5: ✅ Backend Production Readiness Checklist

1.  [ ] **Fix Friend Activity Fan-Out:** Replace the iterative `get_friend_activity` with a denormalized timeline or optimized query.
2.  [ ] **Seed the Shuffle:** Ensure `random.shuffle` is seeded with `session_id` to prevent feed jitter.
3.  [ ] **Refactor Hydrator:** Switch from loading `master_content.json` to using Redis/Firestore key-value lookups.
4.  [ ] **Optimize Index Pool:** Move ranking/sorting logic to Redis Sorted Sets (`ZSET`) instead of in-memory Python sorting.
5.  [ ] **Implement Distributed Session Store:** Remove local memory fallback in `DeduplicationService` and enforce Redis usage.
6.  [ ] **Rate Limit & Circuit Breakers:** Add circuit breakers for Firestore and Redis to prevent cascading failures.
7.  [ ] **Proper Analytics Aggregation:** Stop writing every view to `seen_items` in real-time; batch updates or use a time-series approach.
