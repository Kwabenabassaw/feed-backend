"""
Finishd Feed Backend

FastAPI application entry point.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from .config import get_settings
from .core.logging import setup_logging, get_logger
from .core.exceptions import register_exception_handlers
from .routers import feed_router, analytics_router, search_router, auth_sync_router
from .routers.scheduler import router as scheduler_router
from .services.scheduler import get_scheduler_service

# Initialize
settings = get_settings()
setup_logging()
logger = get_logger(__name__)

# Rate limiter
limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    logger.info(
        "app_startup",
        environment=settings.environment,
        debug=settings.debug
    )
    
    # Start background scheduler (only in production)
    # Check if scheduler should be disabled (for Vercel/serverless)
    import os
    disable_scheduler = os.environ.get("DISABLE_SCHEDULER", "false").lower() == "true"
    
    if settings.environment == "production" and not disable_scheduler:
        scheduler_service = get_scheduler_service()
        scheduler_service.start()
        logger.info("scheduler_auto_started")
        
        # Auto-seed content on startup (handles ephemeral filesystem)
        import asyncio
        asyncio.create_task(_auto_seed_on_startup())
    elif disable_scheduler:
        logger.info("scheduler_disabled_serverless_mode")
    
    yield
    
    # Cleanup on shutdown
    if settings.environment == "production" and not disable_scheduler:
        scheduler_service = get_scheduler_service()
        scheduler_service.stop()
    
    logger.info("app_shutdown")


async def _auto_seed_on_startup():
    """
    Auto-seed content when the app starts.
    
    Handles Render's ephemeral filesystem by:
    1. Running ingestion to fetch fresh content
    2. Running indexer to generate indices
    3. Uploading to Supabase Storage
    
    This runs in the background so it doesn't block startup.
    """
    import asyncio
    from pathlib import Path
    
    # Wait a bit for app to fully start
    await asyncio.sleep(5)
    
    indexes_dir = Path("indexes")
    master_content = indexes_dir / "master_content.json"
    
    # Only seed if master_content.json is missing (fresh deploy)
    if not master_content.exists():
        print("\n" + "="*60)
        print("[AUTO-SEED] 🚀 Fresh deploy detected - seeding content...")
        print("="*60)
        
        try:
            scheduler_service = get_scheduler_service()
            
            # Step 1: Ingestion
            print("[AUTO-SEED] Step 1/2: Running ingestion...")
            await scheduler_service.trigger_ingestion_now()
            
            # Step 2: Indexer + Upload
            print("[AUTO-SEED] Step 2/2: Running indexer + upload...")
            await scheduler_service.trigger_indexer_now()
            
            print("[AUTO-SEED] ✅ Auto-seed complete!")
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"[AUTO-SEED] ❌ Error during auto-seed: {e}")
    else:
        print(f"[AUTO-SEED] ✓ Content already exists, skipping auto-seed")


# Create FastAPI app
app = FastAPI(
    title="Finishd Feed Backend",
    description="Generator & Hydrator Architecture for personalized feeds",
    version="1.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else ["https://finishd.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register exception handlers
register_exception_handlers(app)

# Include routers
app.include_router(feed_router)
app.include_router(analytics_router)
app.include_router(scheduler_router)
app.include_router(search_router)
app.include_router(auth_sync_router)


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "Finishd Feed Backend",
        "version": "1.1.0",
        "status": "running",
        "docs": "/docs" if settings.debug else "disabled",
        "scheduler": "enabled" if settings.environment == "production" else "manual",
    }


@app.get("/health")
async def health():
    """Health check for load balancers."""
    return {"status": "healthy"}

