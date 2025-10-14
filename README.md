# Foreign Films Calendar for Perm Cinemas

[![CI](https://github.com/MaxYtre/movie/actions/workflows/ci.yml/badge.svg)](https://github.com/MaxYtre/movie/actions/workflows/ci.yml)
[![Daily Update](https://github.com/MaxYtre/movie/actions/workflows/daily_update.yml/badge.svg)](https://github.com/MaxYtre/movie/actions/workflows/daily_update.yml)
[![codecov](https://codecov.io/gh/MaxYtre/movie/branch/main/graph/badge.svg)](https://codecov.io/gh/MaxYtre/movie)

A production-ready scraper that monitors Perm cinema listings and generates an ICS calendar of foreign films for easy subscription in calendar apps like Google Calendar.

## Features

- 🎬 **Smart Film Detection**: Automatically identifies and filters foreign (non-Russian) films
- 📅 **ICS Calendar Export**: Generates standard calendar format for universal compatibility  
- 🔄 **Daily Updates**: Automated scraping with intelligent deduplication
- 🚫 **Anti-Blocking**: Respectful scraping with rate limiting and retry logic
- 🗃️ **Persistent Storage**: SQLite database with 30-day event deduplication
- 📊 **Health Monitoring**: Built-in metrics and health endpoints
- 🧪 **Full Test Coverage**: Comprehensive unit and integration tests

## Quick Start

### Subscribe to Calendar

**ICS Feed URL**: `https://maxytree.github.io/movie/calendar.ics`

1. Copy the URL above
2. In Google Calendar: "Add calendar" → "From URL" → Paste URL
3. In Apple Calendar: "File" → "New Calendar Subscription" → Paste URL
4. In Outlook: "Add Calendar" → "Subscribe from web" → Paste URL

### Local Development

```bash
# Clone and setup
git clone https://github.com/MaxYtre/movie.git
cd movie
python -m pip install -e ".[dev]"

# Run locally
python -m movie_scraper.main --mode web
# Access: http://localhost:8000/calendar.ics

# Run scraper once
python -m movie_scraper.main --mode scrape

# Run tests
pytest --cov=movie_scraper

# Code quality checks
ruff check movie_scraper/
mypy movie_scraper/
```

## Configuration

Environment variables (all optional):

```bash
# Core settings
MOVIE_SCRAPER_LOG_LEVEL=INFO          # DEBUG, INFO, WARNING, ERROR
MOVIE_SCRAPER_DB_PATH=data/movies.db  # SQLite database path
MOVIE_SCRAPER_RATE_LIMIT=1.0          # Seconds between requests
MOVIE_SCRAPER_MAX_RETRIES=3           # HTTP retry attempts

# Web server (if running in web mode)
MOVIE_SCRAPER_HOST=0.0.0.0            # Bind address
MOVIE_SCRAPER_PORT=8000               # Port number

# Advanced scraping
MOVIE_SCRAPER_USER_AGENT="Mozilla/5.0 (compatible; movie-scraper/1.0)"  
MOVIE_SCRAPER_ENABLE_JS=false         # Use Playwright for JS-heavy pages
MOVIE_SCRAPER_PROXY_URL=""            # Optional proxy (http://user:pass@host:port)
```

## Architecture

### Components

- **Scraper Engine** (`scraper/`): Handles afisha.ru parsing with robust error handling
- **Film Parser** (`parsers/`): Extracts film details and determines origin country
- **Database Layer** (`database.py`): SQLite with event deduplication and persistence
- **ICS Generator** (`calendar_generator.py`): Standards-compliant calendar export
- **Web Server** (`web/`): FastAPI service with health and metrics endpoints
- **Scheduler** (`.github/workflows/`): Daily GitHub Actions automation

### Data Flow

1. **Discovery**: Scrape `/prm/schedule_cinema/` and paginated pages
2. **Extraction**: Parse each film's detail page for metadata
3. **Filtering**: Exclude Russian films, find next screening date
4. **Deduplication**: Check 30-day publication window per film
5. **Generation**: Create ICS calendar with all-day events
6. **Publication**: Serve via FastAPI and GitHub Pages

### Database Schema

```sql
-- Films with full metadata
CREATE TABLE films (
    id INTEGER PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,          -- afisha.ru URL slug
    title TEXT NOT NULL,
    country TEXT NOT NULL,               -- Production country
    rating TEXT,                         -- Film rating
    description TEXT,
    poster_url TEXT,
    age_limit TEXT,                      -- "12+", "16+", etc.
    last_seen TIMESTAMP,                 -- Latest scrape detection
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Calendar events with publication tracking
CREATE TABLE published_events (
    id INTEGER PRIMARY KEY,
    film_slug TEXT NOT NULL,
    event_date DATE NOT NULL,            -- All-day event date
    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (film_slug) REFERENCES films(slug),
    UNIQUE(film_slug, event_date)
);

-- Screening sessions from cinema pages
CREATE TABLE screening_sessions (
    id INTEGER PRIMARY KEY,
    film_slug TEXT NOT NULL,
    cinema_name TEXT,
    session_datetime TIMESTAMP NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (film_slug) REFERENCES films(slug)
);
```

## Production Deployment

### GitHub Actions (Automatic)

The repository includes automated daily updates:

- **Daily at 06:00 UTC**: Scrapes new films and updates calendar
- **On push to main**: Runs full test suite and validation
- **Publishes to**: GitHub Pages at `https://maxytree.github.io/movie/calendar.ics`

### Manual Deployment

```bash
# Docker deployment
docker build -t movie-scraper .
docker run -d -p 8000:8000 \
  -v ./data:/app/data \
  -e MOVIE_SCRAPER_LOG_LEVEL=INFO \
  movie-scraper

# Production server
pip install -e .
export MOVIE_SCRAPER_LOG_LEVEL=WARNING
export MOVIE_SCRAPER_DB_PATH=/var/lib/movie-scraper/movies.db
python -m movie_scraper.main --mode web
```

### Scaling to PostgreSQL

For high-volume deployments:

```python
# In settings.py - add PostgreSQL support
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/movies.db')

# Migration guide:
# 1. Export SQLite: sqlite3 movies.db .dump > backup.sql
# 2. Convert schema: sqlite_to_postgres.py backup.sql
# 3. Set DATABASE_URL=postgresql://user:pass@host:port/dbname
# 4. Run: alembic upgrade head
```

## API Endpoints

- `GET /calendar.ics` - Full ICS calendar download
- `GET /health` - Service health and database status
- `GET /metrics` - Prometheus-style metrics
- `GET /films` - JSON list of current films (admin)
- `POST /scrape` - Trigger manual scrape (admin)

## Monitoring & Alerts

### Health Checks

```bash
# Basic health
curl https://your-domain/health
# Returns: {"status": "ok", "films_count": 23, "last_update": "2025-10-14T06:30:00Z"}

# Detailed metrics
curl https://your-domain/metrics
# Returns Prometheus format for Grafana integration
```

### Recommended Alerts

- **Scrape Failures**: No successful scrape in 48+ hours
- **Empty Results**: Zero films found for 7+ days  
- **HTTP Errors**: 5xx errors on calendar endpoint
- **Database Growth**: Rapid growth indicating scraping issues

## Development

### Code Quality

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Pre-commit hooks (recommended)
pre-commit install

# Manual checks
ruff check movie_scraper/ tests/          # Linting
ruff format movie_scraper/ tests/         # Formatting  
mypy movie_scraper/                       # Type checking
pytest --cov=movie_scraper tests/        # Tests + coverage
```

### Testing

- **Unit Tests**: Individual component validation
- **Integration Tests**: End-to-end scraping simulation
- **Contract Tests**: ICS format validation
- **Performance Tests**: Load testing for web endpoints

### Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Make changes with tests: `pytest --cov=movie_scraper`
4. Submit pull request with clear description

## Legal & Ethics

- **Respectful Scraping**: 1-second delays, retry backoff, robots.txt compliance
- **Fair Use**: Educational/personal calendar creation only
- **No Redistribution**: Calendar for personal use, not commercial resale
- **Attribution**: Source data credited to afisha.ru

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/MaxYtre/movie/issues)
- **Discussions**: [GitHub Discussions](https://github.com/MaxYtre/movie/discussions)
- **Security**: Email security issues privately

---

**Calendar URL**: `https://maxytree.github.io/movie/calendar.ics`