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
from .routers import feed_router, analytics_router
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
    if settings.environment == "production":
        scheduler_service = get_scheduler_service()
        scheduler_service.start()
        logger.info("scheduler_auto_started")
    
    yield
    
    # Cleanup on shutdown
    if settings.environment == "production":
        scheduler_service = get_scheduler_service()
        scheduler_service.stop()
    
    logger.info("app_shutdown")


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

