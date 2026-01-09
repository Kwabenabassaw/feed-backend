"""
Seed Data Script

Generates mock index files for local development.
Run this to immediately test the frontend without waiting for ingestion.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

# Real YouTube video IDs (trailers that actually work)
SAMPLE_YOUTUBE_IDS = [
    "dQw4w9WgXcQ",  # Rick Astley (test video)
    "JfVOs4VSpmA",  # Spider-Man: No Way Home
    "8g18jFHCLXk",  # Dune 2
    "X0tOpBuYasI",  # Deadpool 3
    "oPiHLBVZvvc",  # Deadpool & Wolverine  
    "mrwBNho7kPI",  # Barbie
    "giXco2jaZ_4",  # Oppenheimer
    "QdrCHMtb-hM",  # Kung Fu Panda 4
    "xEQP4VVuyrY",  # Ghostbusters: Afterlife
    "CaimKeDcudo",  # Beetlejuice 2
    "u34gHaRiBIU",  # Joker 2
    "5khzPJ7_Wpc",  # Venom 3
    "TnGl01FkMMo",  # Avengers: Endgame
    "YoHD9XEInc0",  # Incredibles 2
    "aEYViV8p-Ow",  # Inside Out 2
    "ODPLqTHFkzU",  # Kraven
]

GENRES = ["action", "comedy", "drama", "horror", "thriller", "romance", "scifi", "fantasy", "animation"]

TITLES = [
    "Epic Action Movie", "Hilarious Comedy", "Emotional Drama", "Scary Horror",
    "Intense Thriller", "Romantic Story", "Sci-Fi Adventure", "Fantasy Epic",
    "Animated Feature", "Mystery Film", "War Drama", "Sports Movie"
]


def generate_index_item(item_id: str, tags: list[str], base_score: float) -> dict:
    """Generate a single index item."""
    return {
        "id": item_id,
        "score": round(base_score + random.uniform(-10, 10), 1),
        "tags": tags,
        "timestamp": (datetime.utcnow() - timedelta(hours=random.randint(0, 72))).isoformat(),
        "tmdbId": random.randint(100000, 999999),
        "mediaType": random.choice(["movie", "tv"])
    }


def generate_content_item(item_id: str, tags: list[str]) -> dict:
    """Generate a full content dictionary item."""
    return {
        "id": item_id,
        "youtubeKey": item_id,
        "tmdbId": random.randint(100000, 999999),
        "mediaType": random.choice(["movie", "tv"]),
        "title": f"{random.choice(TITLES)} - {random.choice(tags).title()}",
        "overview": f"An exciting {tags[0]} experience that will keep you on the edge of your seat.",
        "posterPath": f"/poster_{item_id[:8]}.jpg",
        "backdropPath": f"/backdrop_{item_id[:8]}.jpg",
        "videoType": "trailer",
        "contentType": random.choice(["trailer", "teaser", "clip"]),
        "genres": tags,
        "popularity": round(random.uniform(50, 100), 1),
        "voteAverage": round(random.uniform(6.0, 9.5), 1),
        "releaseDate": f"202{random.randint(3, 5)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        "source": "trending",
        "reason": f"Trending in {tags[0].title()}"
    }


def seed_trending():
    """Generate global trending index."""
    items = []
    for i, video_id in enumerate(SAMPLE_YOUTUBE_IDS):
        tags = random.sample(GENRES, random.randint(1, 3))
        score = 90 - (i * 2)  # Descending scores
        items.append(generate_index_item(video_id, tags, score))
    
    # Add more items with generated IDs
    for i in range(20):
        fake_id = f"fake_{i:04d}"
        tags = random.sample(GENRES, random.randint(1, 3))
        score = 70 - i
        items.append(generate_index_item(fake_id, tags, score))
    
    return items


def seed_genre(genre: str):
    """Generate genre-specific index."""
    items = []
    
    # Use some real IDs
    for video_id in random.sample(SAMPLE_YOUTUBE_IDS, min(5, len(SAMPLE_YOUTUBE_IDS))):
        items.append(generate_index_item(video_id, [genre], random.uniform(70, 95)))
    
    # Add genre-specific fake items
    for i in range(15):
        fake_id = f"{genre}_{i:04d}"
        items.append(generate_index_item(fake_id, [genre], random.uniform(40, 80)))
    
    return sorted(items, key=lambda x: x["score"], reverse=True)


def seed_community():
    """Generate community hot posts index."""
    items = []
    for i in range(20):
        post_id = f"post_{i:04d}"
        tags = ["community"] + random.sample(GENRES, 1)
        items.append(generate_index_item(post_id, tags, random.uniform(30, 70)))
    return sorted(items, key=lambda x: x["score"], reverse=True)


def seed_content_dictionary():
    """Generate master content dictionary."""
    items = []
    
    # Real videos
    for video_id in SAMPLE_YOUTUBE_IDS:
        tags = random.sample(GENRES, random.randint(1, 3))
        items.append(generate_content_item(video_id, tags))
    
    # Fake videos (for testing pagination)
    for i in range(50):
        fake_id = f"fake_{i:04d}"
        tags = random.sample(GENRES, random.randint(1, 3))
        items.append(generate_content_item(fake_id, tags))
    
    # Genre-specific fakes
    for genre in GENRES:
        for i in range(15):
            fake_id = f"{genre}_{i:04d}"
            items.append(generate_content_item(fake_id, [genre]))
    
    return items


def main():
    """Generate all seed data files."""
    # Create indexes directory
    indexes_dir = Path("indexes")
    indexes_dir.mkdir(exist_ok=True)
    
    print("[SEED] Generating seed data...")
    
    # Global trending
    trending = seed_trending()
    (indexes_dir / "global_trending.json").write_text(json.dumps(trending, indent=2))
    print(f"  [OK] global_trending.json ({len(trending)} items)")
    
    # Genre indices
    for genre in GENRES:
        genre_data = seed_genre(genre)
        (indexes_dir / f"genre_{genre}.json").write_text(json.dumps(genre_data, indent=2))
        print(f"  [OK] genre_{genre}.json ({len(genre_data)} items)")
    
    # Community hot
    community = seed_community()
    (indexes_dir / "community_hot.json").write_text(json.dumps(community, indent=2))
    print(f"  [OK] community_hot.json ({len(community)} items)")
    
    # Master content dictionary
    content = seed_content_dictionary()
    (indexes_dir / "master_content.json").write_text(json.dumps(content, indent=2))
    print(f"  [OK] master_content.json ({len(content)} items)")
    
    print("\n[DONE] Seed data generated successfully!")
    print(f"   Location: {indexes_dir.absolute()}")
    print("\n   Next steps:")
    print("   1. Start Redis: docker-compose up -d redis")
    print("   2. Run server: uvicorn app.main:app --reload --port 8001")


if __name__ == "__main__":
    main()
