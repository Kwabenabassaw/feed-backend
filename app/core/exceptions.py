"""
Global Exception Handlers

Custom exceptions and FastAPI exception handlers.
"""

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse


class FeedBackendException(Exception):
    """Base exception for feed backend errors."""
    
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(FeedBackendException):
    """Resource not found."""
    
    def __init__(self, resource: str, resource_id: str):
        super().__init__(
            message=f"{resource} not found: {resource_id}",
            status_code=404
        )


class UnauthorizedError(FeedBackendException):
    """Authentication/authorization failed."""
    
    def __init__(self, message: str = "Unauthorized"):
        super().__init__(message=message, status_code=401)


class QuotaExceededError(FeedBackendException):
    """API quota exceeded."""
    
    def __init__(self, api_name: str):
        super().__init__(
            message=f"{api_name} API quota exceeded. Try again tomorrow.",
            status_code=429
        )


class RateLimitError(FeedBackendException):
    """Rate limit exceeded."""
    
    def __init__(self):
        super().__init__(
            message="Rate limit exceeded. Please slow down.",
            status_code=429
        )


async def feed_exception_handler(
    request: Request, 
    exc: FeedBackendException
) -> JSONResponse:
    """Handle FeedBackendException and return JSON response."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "message": exc.message,
            "status_code": exc.status_code,
        }
    )


def register_exception_handlers(app):
    """Register all exception handlers with the FastAPI app."""
    app.add_exception_handler(FeedBackendException, feed_exception_handler)
