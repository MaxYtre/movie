import asyncio
import pytest
import pytest_asyncio
from pathlib import Path
from datetime import datetime, timedelta, date

from movie_scraper.database import Database
from movie_scraper.models import Film, ScreeningSession, PublishedEvent

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db(tmp_path: Path):
    """Provides an initialized file-based database instance for each test."""
    # Use a unique file-based database for each test to ensure isolation.
    db_path = tmp_path / "test.db"
    db_instance = Database(db_path=db_path)
    await db_instance.initialize()
    yield db_instance
    await db_instance.close()


async def test_database_initialization(db: Database):
    """Test that the database initializes correctly and creates tables."""
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in await cursor.fetchall()}
        assert "films" in tables
        assert "screening_sessions" in tables
        assert "published_events" in tables
        assert "schema_version" in tables


async def test_save_film_insert_and_update(db: Database):
    """Test saving a new film and then updating it."""
    film = Film(
        slug="test-film",
        title="Test Film",
        country="Neverland",
        description="A test film.",
        source_url="http://example.com/test-film"
    )

    # 1. Insert new film
    saved_film = await db.save_film(film)
    assert saved_film.id is not None
    assert saved_film.created_at is not None
    assert saved_film.last_seen is not None
    assert saved_film.title == "Test Film"

    # Verify it's in the DB
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT title, country FROM films WHERE slug = ?", ("test-film",))
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "Test Film"
        assert row[1] == "Neverland"

    # 2. Update existing film
    film.title = "Test Film Updated"
    film.country = "Wonderland"
    updated_film = await db.save_film(film)

    assert updated_film.id == saved_film.id
    assert updated_film.title == "Test Film Updated"
    assert updated_film.created_at == saved_film.created_at  # Should not change
    assert updated_film.last_seen > saved_film.last_seen

    # Verify update in DB
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT title, country FROM films WHERE slug = ?", ("test-film",))
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "Test Film Updated"
        assert row[1] == "Wonderland"


async def test_save_screening_session(db: Database):
    """Test saving a screening session."""
    film = Film(slug="test-film", title="Test Film")
    await db.save_film(film)

    session = ScreeningSession(
        film_slug="test-film",
        cinema_name="Test Cinema",
        session_datetime=datetime.now() + timedelta(days=1),
        format_info="2D"
    )

    saved_session = await db.save_screening_session(session)
    assert saved_session.id is not None
    assert saved_session.last_updated is not None

    # Verify it's in the DB
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT cinema_name, format_info FROM screening_sessions WHERE film_slug = ?", ("test-film",))
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "Test Cinema"
        assert row[1] == "2D"


async def test_get_foreign_films_with_upcoming_sessions(db: Database):
    """Test retrieving foreign films with sessions in the future."""
    # Film 1: Foreign, upcoming session
    film1 = Film(slug="foreign-film", title="Foreign Film", country="France")
    await db.save_film(film1)
    await db.save_screening_session(ScreeningSession(film_slug="foreign-film", cinema_name="C1", session_datetime=datetime.now() + timedelta(days=1)))

    # Film 2: Russian, upcoming session (should be excluded)
    film2 = Film(slug="russian-film", title="Russian Film", country="Россия")
    await db.save_film(film2)
    await db.save_screening_session(ScreeningSession(film_slug="russian-film", cinema_name="C2", session_datetime=datetime.now() + timedelta(days=1)))

    # Film 3: Foreign, past session (should be excluded)
    film3 = Film(slug="past-session-film", title="Past Session Film", country="Germany")
    await db.save_film(film3)
    await db.save_screening_session(ScreeningSession(film_slug="past-session-film", cinema_name="C3", session_datetime=datetime.now() - timedelta(days=1)))

    foreign_films = await db.get_foreign_films_with_upcoming_sessions()
    assert len(foreign_films) == 1
    assert foreign_films[0].slug == "foreign-film"
    assert foreign_films[0].next_screening is not None


async def test_film_cache_logic(db: Database):
    """Test the film cache functions: upsert, get, and is_fresh."""
    slug = "cached-film"
    
    # 1. Check freshness for a non-existent film
    assert not await db.is_film_cache_fresh(slug, ttl_days=1)

    # 2. Upsert film data
    await db.upsert_film_cache(
        slug=slug,
        title="Cached Film",
        country="USA",
        rating="8.0",
        description="A cached film.",
        age_limit="18+",
        url="http://example.com/cached",
        year=2023
    )

    # 3. Get the cached data
    cached_data = await db.get_film_cache(slug)
    assert cached_data is not None
    assert cached_data["title"] == "Cached Film"
    assert cached_data["year"] == 2023

    # 4. Check freshness (should be fresh)
    assert await db.is_film_cache_fresh(slug, ttl_days=1)

    # 5. Manipulate time to check staleness
    async with db._get_connection() as conn:
        stale_time = (datetime.utcnow() - timedelta(days=2)).isoformat()
        await conn.execute("UPDATE films SET last_seen = ? WHERE slug = ?", (stale_time, slug))
        await conn.commit()

    assert not await db.is_film_cache_fresh(slug, ttl_days=1)

async def test_event_suppression(db: Database):
    """Test event suppression logic."""
    film_slug = "event-film"
    event_date = date.today()

    # 1. No event recorded, should not be suppressed
    assert not await db.is_event_suppressed(film_slug, event_date)

    # 2. Record an event
    recorded_event = await db.record_published_event(film_slug, event_date)
    assert recorded_event.id is not None
    assert recorded_event.film_slug == film_slug

    # 3. Should now be suppressed
    assert await db.is_event_suppressed(film_slug, event_date, days=30)
    
    # 4. Check suppression with a short window (e.g., 0 days ago)
    # To do this, we need to manipulate the `published_at` timestamp
    async with db._get_connection() as conn:
        past_time = (datetime.now() - timedelta(days=2)).isoformat()
        await conn.execute("UPDATE published_events SET published_at = ? WHERE film_slug = ?", (past_time, film_slug))
        await conn.commit()

    # Should not be suppressed if we only check for events in the last 1 day
    assert not await db.is_event_suppressed(film_slug, event_date, days=1)


async def test_cleanup_old_data(db: Database):
    """Test that old records are cleaned up correctly."""
    # 1. Create old and new data
    now = datetime.now()
    old_date = now - timedelta(days=100)
    
    # Old film and session
    old_film = Film(slug="old-film", title="Old Film", last_seen=old_date)
    await db.save_film(old_film)
    await db.save_screening_session(ScreeningSession(film_slug="old-film", cinema_name="Old Cinema", session_datetime=old_date))
    await db.record_published_event("old-film", old_date.date())
    async with db._get_connection() as conn:
         await conn.execute("UPDATE published_events SET published_at = ? WHERE film_slug = 'old-film'", (old_date.isoformat(),))
         await conn.commit()


    # New film and session
    new_film = Film(slug="new-film", title="New Film", last_seen=now)
    await db.save_film(new_film)
    await db.save_screening_session(ScreeningSession(film_slug="new-film", cinema_name="New Cinema", session_datetime=now))

    # 2. Run cleanup
    cleaned_count = await db.cleanup_old_data(days=90)
    
    # Should clean 1 film, 1 session, 1 event
    assert cleaned_count == 3

    # 3. Verify old data is gone
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM films WHERE slug = 'old-film'")
        assert await cursor.fetchone() is None
        cursor = await conn.execute("SELECT * FROM screening_sessions WHERE film_slug = 'old-film'")
        assert await cursor.fetchone() is None
        cursor = await conn.execute("SELECT * FROM published_events WHERE film_slug = 'old-film'")
        assert await cursor.fetchone() is None

    # 4. Verify new data remains
    async with db._get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM films WHERE slug = 'new-film'")
        assert await cursor.fetchone() is not None
        cursor = await conn.execute("SELECT * FROM screening_sessions WHERE film_slug = 'new-film'")
        assert await cursor.fetchone() is not None


async def test_get_database_stats(db: Database):
    """Test retrieval of database statistics."""
    # 1. Get stats on empty DB
    stats = await db.get_database_stats()
    assert stats["total_films"] == 0
    assert stats["foreign_films"] == 0
    assert stats["upcoming_sessions"] == 0

    # 2. Add some data
    film1 = Film(slug="film-1", title="Film 1", country="USA")
    await db.save_film(film1)
    film2 = Film(slug="film-2", title="Film 2", country="Россия")
    await db.save_film(film2)
    await db.save_screening_session(ScreeningSession(film_slug="film-1", cinema_name="C1", session_datetime=datetime.now() + timedelta(days=1)))
    await db.record_published_event("film-1", date.today())
    
    # 3. Get stats again
    stats = await db.get_database_stats()
    assert stats["total_films"] == 2
    assert stats["foreign_films"] == 1
    assert stats["upcoming_sessions"] == 1
    assert stats["recent_events"] == 1
    assert stats["db_size_bytes"] == 0  # In-memory DB size is 0
    assert stats["last_update"] is not None
