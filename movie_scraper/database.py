"""
Database layer for the movie scraper application.

Provides async SQLite interface with proper connection management,
migrations, and business logic for film and event storage.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import AsyncGenerator
from typing import List
from typing import Optional

import aiosqlite

from movie_scraper.models import Film
from movie_scraper.models import PublishedEvent
from movie_scraper.models import ScreeningSession
from movie_scraper.settings import get_settings

logger = logging.getLogger(__name__)


class Database:
    """
    Async SQLite database interface for movie scraper.
    
    Handles all database operations including schema creation, migrations,
    and business logic for storing films, screening sessions, and tracking
    published calendar events.
    """
    
    def __init__(self, db_path: Optional[Path] = None) -> None:
        """Initialize database with optional custom path."""
        self.db_path = db_path or get_settings().db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize database schema and run migrations."""
        async with self._lock:
            await self._ensure_schema()
            await self._run_migrations()
            logger.info(f"Database initialized at {self.db_path}")
    
    async def close(self) -> None:
        """Close database connection cleanly."""
        async with self._lock:
            if self._connection:
                await self._connection.close()
                self._connection = None
    
    @asynccontextmanager
    async def _get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Get database connection with proper lifecycle management."""
        async with self._lock:
            if not self._connection:
                self._connection = await aiosqlite.connect(
                    self.db_path,
                    timeout=30.0,
                    isolation_level=None,  # Autocommit mode
                )
                
                # Configure SQLite for better performance and reliability
                await self._connection.execute("PRAGMA foreign_keys = ON")
                await self._connection.execute("PRAGMA journal_mode = WAL")
                await self._connection.execute("PRAGMA synchronous = NORMAL")
                await self._connection.execute("PRAGMA cache_size = -64000")  # 64MB cache
                
            yield self._connection
    
    async def _ensure_schema(self) -> None:
        """Create database tables if they don't exist."""
        schema_sql = """
        -- Films table with comprehensive metadata
        CREATE TABLE IF NOT EXISTS films (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            country TEXT NOT NULL,
            rating TEXT,
            description TEXT,
            poster_url TEXT,
            age_limit TEXT,
            source_url TEXT,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Screening sessions for calculating next showings
        CREATE TABLE IF NOT EXISTS screening_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            film_slug TEXT NOT NULL,
            cinema_name TEXT NOT NULL,
            session_datetime TIMESTAMP NOT NULL,
            format_info TEXT,
            language TEXT,
            price_range TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (film_slug) REFERENCES films(slug) ON DELETE CASCADE
        );
        
        -- Published events for 30-day deduplication
        CREATE TABLE IF NOT EXISTS published_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            film_slug TEXT NOT NULL,
            event_date DATE NOT NULL,
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (film_slug) REFERENCES films(slug) ON DELETE CASCADE,
            UNIQUE(film_slug, event_date)
        );
        
        -- Indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_films_slug ON films(slug);
        CREATE INDEX IF NOT EXISTS idx_films_country ON films(country);
        CREATE INDEX IF NOT EXISTS idx_films_last_seen ON films(last_seen);
        CREATE INDEX IF NOT EXISTS idx_sessions_film_datetime ON screening_sessions(film_slug, session_datetime);
        CREATE INDEX IF NOT EXISTS idx_events_film_date ON published_events(film_slug, event_date);
        CREATE INDEX IF NOT EXISTS idx_events_published_at ON published_events(published_at);
        """
        
        async with self._get_connection() as conn:
            await conn.executescript(schema_sql)
            await conn.commit()
    
    async def _run_migrations(self) -> None:
        """Run database migrations if needed."""
        # Check current schema version
        async with self._get_connection() as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            has_version_table = await cursor.fetchone() is not None
            
            if not has_version_table:
                # Create version table and set initial version
                await conn.execute(
                    "CREATE TABLE schema_version (version INTEGER PRIMARY KEY)"
                )
                await conn.execute("INSERT INTO schema_version (version) VALUES (1)")
                await conn.commit()
                logger.info("Database schema initialized to version 1")
            
            # Future migrations can be added here as needed
            # Example:
            # current_version = await self._get_schema_version()
            # if current_version < 2:
            #     await self._migrate_to_version_2(conn)
    
    async def save_film(self, film: Film) -> Film:
        """
        Save or update a film in the database.
        
        Args:
            film: Film object to save
            
        Returns:
            Film object with updated ID and timestamps
        """
        async with self._get_connection() as conn:
            # Check if film already exists
            cursor = await conn.execute(
                "SELECT id, created_at FROM films WHERE slug = ?",
                (film.slug,)
            )
            existing = await cursor.fetchone()
            
            if existing:
                # Update existing film
                film.id = existing[0]
                film.created_at = datetime.fromisoformat(existing[1]) if existing[1] else None
                film.last_seen = datetime.now()
                
                await conn.execute(
                    """
                    UPDATE films SET
                        title = ?, country = ?, rating = ?, description = ?,
                        poster_url = ?, age_limit = ?, source_url = ?, last_seen = ?
                    WHERE slug = ?
                    """,
                    (
                        film.title, film.country, film.rating, film.description,
                        str(film.poster_url) if film.poster_url else None,
                        film.age_limit, str(film.source_url) if film.source_url else None,
                        film.last_seen.isoformat(), film.slug
                    )
                )
                logger.debug(f"Updated existing film: {film.slug}")
            else:
                # Insert new film
                film.created_at = datetime.now()
                film.last_seen = datetime.now()
                
                cursor = await conn.execute(
                    """
                    INSERT INTO films (slug, title, country, rating, description, poster_url, age_limit, source_url, created_at, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        film.slug, film.title, film.country, film.rating, film.description,
                        str(film.poster_url) if film.poster_url else None,
                        film.age_limit, str(film.source_url) if film.source_url else None,
                        film.created_at.isoformat(), film.last_seen.isoformat()
                    )
                )
                film.id = cursor.lastrowid
                logger.debug(f"Inserted new film: {film.slug}")
            
            await conn.commit()
            return film
    
    async def save_screening_session(self, session: ScreeningSession) -> ScreeningSession:
        """Save a screening session to the database."""
        async with self._get_connection() as conn:
            session.last_updated = datetime.now()
            
            # Use INSERT OR REPLACE to handle duplicates
            cursor = await conn.execute(
                """
                INSERT OR REPLACE INTO screening_sessions 
                (film_slug, cinema_name, session_datetime, format_info, language, price_range, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.film_slug, session.cinema_name, 
                    session.session_datetime.isoformat(),
                    session.format_info, session.language, session.price_range,
                    session.last_updated.isoformat()
                )
            )
            
            if not session.id:
                session.id = cursor.lastrowid
            
            await conn.commit()
            return session
    
    async def get_foreign_films_with_upcoming_sessions(self) -> List[Film]:
        """
        Get all foreign films that have upcoming screening sessions.
        
        Returns:
            List of foreign films with next_screening populated
        """
        async with self._get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT f.*, MIN(s.session_datetime) as next_screening
                FROM films f
                JOIN screening_sessions s ON f.slug = s.film_slug
                WHERE f.country != 'Россия' 
                  AND f.country != 'Russia'
                  AND f.country != 'СССР'
                  AND s.session_datetime > datetime('now')
                GROUP BY f.slug
                ORDER BY next_screening
                """
            )
            
            films = []
            async for row in cursor:
                film_data = {
                    "id": row[0],
                    "slug": row[1],
                    "title": row[2],
                    "country": row[3],
                    "rating": row[4],
                    "description": row[5],
                    "poster_url": row[6],
                    "age_limit": row[7],
                    "source_url": row[8],
                    "last_seen": datetime.fromisoformat(row[9]) if row[9] else None,
                    "created_at": datetime.fromisoformat(row[10]) if row[10] else None,
                    "next_screening": datetime.fromisoformat(row[11]) if row[11] else None,
                }
                films.append(Film(**film_data))
            
            logger.info(f"Found {len(films)} foreign films with upcoming sessions")
            return films
    
    async def is_event_suppressed(self, film_slug: str, event_date: date, days: int = 30) -> bool:
        """
        Check if an event should be suppressed due to recent publication.
        
        Args:
            film_slug: Film identifier
            event_date: Proposed event date
            days: Number of days to check for suppression
            
        Returns:
            True if event should be suppressed
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        async with self._get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT COUNT(*) FROM published_events
                WHERE film_slug = ? AND published_at > ?
                """,
                (film_slug, cutoff_date.isoformat())
            )
            count = await cursor.fetchone()
            
        return (count[0] if count else 0) > 0
    
    async def record_published_event(self, film_slug: str, event_date: date) -> PublishedEvent:
        """
        Record that an event has been published for a film.
        
        Args:
            film_slug: Film identifier
            event_date: Date of the event
            
        Returns:
            PublishedEvent object
        """
        event = PublishedEvent(
            film_slug=film_slug,
            event_date=event_date,
            published_at=datetime.now()
        )
        
        async with self._get_connection() as conn:
            cursor = await conn.execute(
                """
                INSERT OR REPLACE INTO published_events (film_slug, event_date, published_at)
                VALUES (?, ?, ?)
                """,
                (event.film_slug, event.event_date.isoformat(), event.published_at.isoformat())
            )
            
            event.id = cursor.lastrowid
            await conn.commit()
            
        logger.debug(f"Recorded published event for {film_slug} on {event_date}")
        return event
    
    async def cleanup_old_data(self, days: int = 90) -> int:
        """
        Clean up old screening sessions and stale film records.
        
        Args:
            days: Number of days to retain data
            
        Returns:
            Number of records cleaned up
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        cleaned_count = 0
        
        async with self._get_connection() as conn:
            # Remove old screening sessions
            cursor = await conn.execute(
                "DELETE FROM screening_sessions WHERE session_datetime < ?",
                (cutoff_date.isoformat(),)
            )
            cleaned_count += cursor.rowcount
            
            # Remove films not seen recently
            cursor = await conn.execute(
                "DELETE FROM films WHERE last_seen < ?",
                (cutoff_date.isoformat(),)
            )
            cleaned_count += cursor.rowcount
            
            # Clean up orphaned published events
            cursor = await conn.execute(
                "DELETE FROM published_events WHERE published_at < ?",
                ((datetime.now() - timedelta(days=days*2)).isoformat(),)
            )
            cleaned_count += cursor.rowcount
            
            await conn.commit()
            
            # Vacuum database to reclaim space
            await conn.execute("VACUUM")
            
        logger.info(f"Cleaned up {cleaned_count} old database records")
        return cleaned_count
    
    async def get_database_stats(self) -> dict:
        """
        Get database statistics for monitoring and health checks.
        
        Returns:
            Dictionary with database metrics
        """
        async with self._get_connection() as conn:
            stats = {}
            
            # Count films by type
            cursor = await conn.execute("SELECT COUNT(*) FROM films")
            stats["total_films"] = (await cursor.fetchone())[0]
            
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM films WHERE country NOT IN ('Россия', 'Russia', 'СССР')"
            )
            stats["foreign_films"] = (await cursor.fetchone())[0]
            
            # Count upcoming sessions
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM screening_sessions WHERE session_datetime > datetime('now')"
            )
            stats["upcoming_sessions"] = (await cursor.fetchone())[0]
            
            # Count recent events
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM published_events WHERE published_at > datetime('now', '-30 days')"
            )
            stats["recent_events"] = (await cursor.fetchone())[0]
            
            # Database file size
            stats["db_size_bytes"] = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            # Last update time
            cursor = await conn.execute("SELECT MAX(last_seen) FROM films")
            last_update = await cursor.fetchone()
            stats["last_update"] = last_update[0] if last_update[0] else None
            
        return stats
    
    async def __aenter__(self) -> "Database":
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()