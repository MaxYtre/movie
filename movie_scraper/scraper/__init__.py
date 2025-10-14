"""
Web scraping components for the movie scraper application.

This package contains all scraping-related functionality including:
- Site-specific scrapers (afisha.ru)
- HTTP client management with rate limiting
- Content parsing and extraction
- Error handling and retry logic
"""

from movie_scraper.scraper.afisha_scraper import AfishaScraper
from movie_scraper.scraper.http_client import HttpClient

__all__ = ["AfishaScraper", "HttpClient"]