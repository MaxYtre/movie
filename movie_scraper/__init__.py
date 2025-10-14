"""
Foreign Films Calendar Scraper for Perm Cinemas.

A production-ready web scraper that monitors cinema listings in Perm, Russia,
filters foreign (non-Russian) films, and generates ICS calendar files for
easy subscription in calendar applications.

Main Components:
- Scraper: Handles afisha.ru parsing with robust error handling
- Parser: Extracts film metadata and determines production country
- Database: SQLite storage with deduplication logic
- Calendar: ICS generation following RFC 5545 standards
- Web Server: FastAPI service with health monitoring
- Scheduler: Daily automation via GitHub Actions

Usage:
    # Run web server
    python -m movie_scraper.main --mode web
    
    # Run scraper once
    python -m movie_scraper.main --mode scrape
    
    # Get calendar programmatically
    from movie_scraper.calendar_generator import CalendarGenerator
    from movie_scraper.database import Database
    
    db = Database()
    generator = CalendarGenerator(db)
    ics_content = await generator.generate_calendar()
"""

__version__ = "1.0.0"
__author__ = "MaxYtre"
__email__ = "maxytree@example.com"
__license__ = "MIT"

# Public API for external usage
from movie_scraper.calendar_generator import CalendarGenerator
from movie_scraper.database import Database
from movie_scraper.models import Film
from movie_scraper.models import PublishedEvent
from movie_scraper.models import ScreeningSession
from movie_scraper.scraper.afisha_scraper import AfishaScraper

__all__ = [
    "CalendarGenerator",
    "Database",
    "Film",
    "PublishedEvent", 
    "ScreeningSession",
    "AfishaScraper",
]