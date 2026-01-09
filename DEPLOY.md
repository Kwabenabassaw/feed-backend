# Finishd Feed Backend Deployment Guide

## Quick Deploy Options

### Option 1: Railway (Recommended - Easiest)

1. **Connect Repository**
   ```bash
   # Push to GitHub first
   cd feed-backend
   git init
   git add .
   git commit -m "Initial feed backend"
   git remote add origin <your-repo-url>
   git push -u origin main
   ```

2. **Deploy on Railway**
   - Go to [railway.app](https://railway.app)
   - Click "New Project" → "Deploy from GitHub repo"
   - Select your repository
   - Railway auto-detects `railway.toml` and `Dockerfile`

3. **Add Environment Variables**
   In Railway dashboard, go to Variables and add:
   ```
   ENVIRONMENT=production
   DEBUG=false
   FIREBASE_CREDENTIALS_PATH=/app/service-account.json
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your-anon-key
   YOUTUBE_API_KEY=your-key
   TMDB_API_KEY=your-key
   TMDB_READ_ACCESS_TOKEN=your-token
   REDIS_URL=redis://default:password@host:port
   ```

4. **Add Redis**
   - In Railway dashboard → "New" → "Database" → "Redis"
   - Copy the connection URL to `REDIS_URL`

---

### Option 2: Google Cloud Run

1. **Install gcloud CLI**
   ```bash
   # Windows (PowerShell as Admin)
   (New-Object Net.WebClient).DownloadFile("https://sdk.cloud.google.com/google-cloud-sdk.zip", "google-cloud-sdk.zip")
   Expand-Archive google-cloud-sdk.zip -DestinationPath .
   .\google-cloud-sdk\install.bat
   ```

2. **Build & Deploy**
   ```bash
   cd feed-backend
   
   # Authenticate
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   
   # Build container
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/feed-backend
   
   # Deploy
   gcloud run deploy feed-backend \
     --image gcr.io/YOUR_PROJECT_ID/feed-backend \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars "ENVIRONMENT=production,DEBUG=false"
   ```

3. **Set Environment Variables**
   ```bash
   gcloud run services update feed-backend \
     --set-env-vars "SUPABASE_URL=...,SUPABASE_KEY=..."
   ```

---

### Option 3: Render

1. Create `render.yaml` (already included)
2. Go to [render.com](https://render.com)
3. New → Web Service → Connect repo
4. Render auto-detects configuration

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `ENVIRONMENT` | Yes | `development` or `production` |
| `DEBUG` | Yes | `true` or `false` |
| `FIREBASE_CREDENTIALS_PATH` | Yes | Path to service account JSON |
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase anon/service key |
| `YOUTUBE_API_KEY` | No | For ingestion (uses RSS fallback) |
| `TMDB_API_KEY` | Yes | TMDB v3 API key |
| `TMDB_READ_ACCESS_TOKEN` | Yes | TMDB v4 read token |
| `REDIS_URL` | No | Redis connection (falls back to memory) |
| `RATE_LIMIT_PER_MINUTE` | No | Default: 60 |

---

## Post-Deployment Checklist

- [ ] Verify `/health` returns `{"status": "healthy"}`
- [ ] Check `/` shows version `1.1.0`
- [ ] Test `/docs` is disabled (404) in production
- [ ] Trigger initial ingestion: `POST /scheduler/trigger/ingestion`
- [ ] Trigger initial indexer: `POST /scheduler/trigger/indexer`
- [ ] Upload indices: `POST /scheduler/trigger/upload`
- [ ] Verify scheduler is running: `GET /scheduler/status`

---

## Supabase Setup

1. **Create Storage Bucket**
   - Go to Supabase Dashboard → Storage
   - Create bucket named `indexes`
   - Set to **Public** (for fast CDN reads)

2. **Verify Bucket Policies**
   ```sql
   -- Allow public downloads
   CREATE POLICY "Public Access" ON storage.objects
   FOR SELECT USING (bucket_id = 'indexes');
   ```

---

## Redis Setup (Optional but Recommended)

Railway/Render provide managed Redis. For Upstash (free tier):

1. Go to [upstash.com](https://upstash.com)
2. Create Redis database
3. Copy connection URL
4. Set `REDIS_URL` environment variable

---

## Monitoring

- **Railway**: Built-in logging dashboard
- **Cloud Run**: Cloud Logging (`gcloud logs read`)
- **Custom**: Add `/scheduler/status` to health checks
