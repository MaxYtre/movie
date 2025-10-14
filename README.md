# Foreign Films Calendar for Perm Cinemas

A GitHub‑hosted scraper that monitors Perm cinema listings on afisha.ru and generates an ICS calendar of foreign (non‑Russian) films. Runs entirely on GitHub Actions; no local runtime required.

## Live links

- Site: https://maxytree.github.io/movie/
- Calendar (raw, direct): https://raw.githubusercontent.com/MaxYtre/movie/main/docs/calendar.ics

## How to subscribe

- Google Calendar: Settings → Add calendar → From URL → paste the raw link above.
- Apple Calendar (macOS): File → New Calendar Subscription → paste the raw link.
- Outlook: Add calendar → Subscribe from web → paste the raw link.

## What it does

- Parses afisha.ru Perm schedule pages and each film’s detail page
- Filters out Russian productions; keeps foreign films only
- Finds the next screening date in Perm
- Publishes an ICS with all‑day events
- Updates daily at 06:00 (repo workflow), also on demand
- Shows diagnostics (REGION/NEW‑SELECTORS/REASON) on the site and in docs/diag.txt

## Performance and caching

- Persistent SQLite cache at data/cache.sqlite committed to main
- Cache TTL: 15 days (env MOVIE_SCRAPER_CACHE_TTL_DAYS)
- On cache hit the scraper skips HTTP (logged as "[CACHE] HIT"), which reduces run time to a few minutes
- DIAG counters show cache effectiveness: cache_hits / cache_misses

## Configuration (env)

- MOVIE_SCRAPER_LOG_LEVEL: DEBUG|INFO (default DEBUG)
- MOVIE_SCRAPER_MAX_FILMS: how many films to scan per run (e.g. 20)
- MOVIE_SCRAPER_RATE_LIMIT: seconds between requests (default 5.0)
- MOVIE_SCRAPER_CACHE_TTL_DAYS: cache freshness window in days (default 15)
- MOVIE_SCRAPER_PROXY_URL: optional HTTP proxy

## Repository layout

- movie_scraper/simple_scraper.py — scraper logic, cache usage, ICS/index generation
- docs/ — published artifacts (index.html, calendar.ics, diag.txt)
- data/cache.sqlite — persistent cache
- .github/workflows/daily-update.yml — daily job (commits docs/ and cache)

## GitHub Pages

- Pages is configured to serve from main /docs
- calendar.ics is also available via raw to avoid Pages propagation delays

## Development notes

- Project is intended to run only on GitHub (Actions); local run isn’t required
- Logs in index.html include the latest DIAG COPY for quick debugging

## License

MIT
