"""
Configuration settings for the movie scraper application.

Centralized configuration using Pydantic Settings for type-safe environment
variable handling. All settings can be overridden via environment variables
with the MOVIE_SCRAPER_ prefix.

Example:
    export MOVIE_SCRAPER_LOG_LEVEL=DEBUG
    export MOVIE_SCRAPER_RATE_LIMIT=2.0
    python -m movie_scraper.main
"""

import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import Field
from pydantic import field_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    
    All settings can be overridden via environment variables with the
    MOVIE_SCRAPER_ prefix (e.g., MOVIE_SCRAPER_LOG_LEVEL=DEBUG).
    """
    
    model_config = SettingsConfigDict(
        env_prefix="MOVIE_SCRAPER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Core application settings
    log_level: str = Field(
        default="DEBUG",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    db_path: Path = Field(
        default=Path("data/movies.db"),
        description="Path to SQLite database file"
    )
    
    # Web server configuration
    host: str = Field(
        default="0.0.0.0",
        description="Host address to bind web server"
    )
    
    port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description="Port number for web server"
    )
    
    # Scraping behavior settings
    rate_limit: float = Field(
        default=1.0,
        ge=0.1,
        le=10.0,
        description="Minimum seconds between HTTP requests"
    )
    
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retry attempts for failed requests"
    )
    
    request_timeout: float = Field(
        default=30.0,
        ge=5.0,
        le=120.0,
        description="HTTP request timeout in seconds"
    )
    
    user_agent: str = Field(
        default="Mozilla/5.0 (compatible; movie-scraper/1.0; +https://github.com/MaxYtre/movie)",
        description="User-Agent string for HTTP requests"
    )
    
    # Optional proxy configuration
    proxy_url: Optional[str] = Field(
        default=None,
        description="Optional proxy URL (http://user:pass@host:port)"
    )
    
    # Advanced scraping options
    enable_js: bool = Field(
        default=False,
        description="Enable JavaScript rendering via Playwright for complex pages"
    )
    
    max_concurrent_requests: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of concurrent HTTP requests"
    )
    
    # Calendar generation settings
    calendar_name: str = Field(
        default="Foreign Films - Perm Cinemas",
        description="Display name for the generated ICS calendar"
    )
    
    calendar_description: str = Field(
        default="Foreign (non-Russian) films currently showing in Perm cinemas. Updated daily.",
        description="Description text for the ICS calendar"
    )
    
    # Database and persistence settings
    event_retention_days: int = Field(
        default=30,
        ge=1,
        le=365,
        description="Number of days to prevent duplicate events for the same film"
    )
    
    max_db_size_mb: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Maximum database size in MB before cleanup"
    )
    
    # Monitoring and health check settings
    health_check_timeout: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Timeout for health check operations"
    )
    
    metrics_enabled: bool = Field(
        default=True,
        description="Enable Prometheus metrics collection"
    )
    
    # Development and debugging
    debug_mode: bool = Field(
        default=False,
        description="Enable debug mode with verbose logging and error traces"
    )
    
    dry_run: bool = Field(
        default=False,
        description="Run scraper without making any database changes"
    )
    
    # External service configuration
    sentry_dsn: Optional[str] = Field(
        default=None,
        description="Sentry DSN for error reporting in production"
    )
    
    # Regional settings for Perm, Russia
    base_url: str = Field(
        default="https://www.afisha.ru",
        description="Base URL for afisha.ru website"
    )
    
    perm_path: str = Field(
        default="/prm/schedule_cinema/",
        description="URL path for Perm cinema listings"
    )
    
    timezone: str = Field(
        default="Asia/Yekaterinburg",
        description="Timezone for Perm (UTC+5)"
    )
    
    # Quality and reliability settings
    circuit_breaker_threshold: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of consecutive failures before activating circuit breaker"
    )
    
    circuit_breaker_timeout: float = Field(
        default=300.0,
        ge=60.0,
        le=3600.0,
        description="Circuit breaker timeout in seconds"
    )
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate that log_level is a valid logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper_v = v.upper()
        if upper_v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got {v}")
        return upper_v
    
    @field_validator("db_path")
    @classmethod  
    def validate_db_path(cls, v: Path) -> Path:
        """Ensure database directory exists."""
        v.parent.mkdir(parents=True, exist_ok=True)
        return v
    
    @property
    def full_base_url(self) -> str:
        """Get the complete base URL for Perm cinema listings."""
        return f"{self.base_url.rstrip('/')}{self.perm_path}"
    
    @property
    def logging_config(self) -> Dict[str, Any]:
        """Get logging configuration dict."""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "structured": {
                    "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": self.log_level,
                    "formatter": "structured" if self.debug_mode else "standard",
                    "stream": "ext://sys.stdout",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "INFO",
                    "formatter": "structured",
                    "filename": "logs/movie_scraper.log",
                    "maxBytes": 10485760,  # 10MB
                    "backupCount": 5,
                },
            },
            "loggers": {
                "movie_scraper": {
                    "level": self.log_level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "aiohttp.access": {
                    "level": "WARNING",
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
            "root": {
                "level": "WARNING",
                "handlers": ["console"],
            },
        }
    
    def setup_logging(self) -> None:
        """Configure application logging based on current settings."""
        import logging.config
        
        # Ensure logs directory exists
        Path("logs").mkdir(exist_ok=True)
        
        # Configure logging
        logging.config.dictConfig(self.logging_config)
        
        # Set up structured logging for production
        if not self.debug_mode:
            import structlog
            
            structlog.configure(
                processors=[
                    structlog.stdlib.filter_by_level,
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.PositionalArgumentsFormatter(),
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.UnicodeDecoder(),
                    structlog.processors.JSONRenderer(),
                ],
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )


# Global settings instance
settings = Settings()

# Convenience function for external usage
def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings