"""
Structured Logging Configuration

Uses structlog for JSON-formatted, structured logs.
"""

import logging
import sys
from typing import Optional

import structlog
from structlog.types import Processor

from ..config import get_settings


def setup_logging(log_level: Optional[str] = None):
    """
    Configure structured logging for the application.
    
    Args:
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR)
    """
    settings = get_settings()
    
    # Determine log level
    if log_level is None:
        log_level = "DEBUG" if settings.debug else "INFO"
    
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )
    
    # Processors for structlog
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]
    
    if settings.environment == "development":
        # Pretty console output for dev
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True)
        ]
    else:
        # JSON output for production
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ]
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = "feed_backend") -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)
