# Finishd Feed Backend

> **Version 1.1.0** | Generator & Hydrator Architecture

High-performance personalized feed system for TikTok-style video content.

## Quick Start

```bash
# Setup
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env

# Generate seed data
python scripts\seed_data.py

# Run server
uvicorn app.main:app --reload --port 8001
```

**API Docs:** http://localhost:8001/docs

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | System design & component details |
| [DEPLOY.md](./DEPLOY.md) | Deployment guide for Railway/Render/Cloud Run |

## Key Features

- **50/30/20 Mixing**: Trending + Personalized + Friend activity
- **Sub-150ms Latency**: Generator & Hydrator separation
- **Cold Start Handling**: Auto-fallback for new users
- **Session Deduplication**: Cursor-based pagination
- **Background Jobs**: APScheduler for content ingestion

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /feed` | Personalized feed (auth required) |
| `POST /analytics/event` | Track user events |
| `GET /scheduler/status` | Job status |
| `POST /scheduler/trigger/*` | Manual job triggers |

## Environment Variables

See [.env.example](./.env.example) for all options.

## License

Proprietary - Finishd Inc.SUPABASE_KEY=your-anon-key
REDIS_URL=redis://localhost:6379
```

## API Endpoints

|--------|------|-------------|
| GET | `/feed` | Get personalized feed (requires auth) |
| POST | `/analytics/event` | Track user interactions |

## Development

```bash
# Run tests
python -m pytest tests/ -v

# Generate seed data
python scripts/seed_data.py
```
