"""
Afisha (Perm) scraper with enrichment integration (development branch).
- Adds trailer/poster/ratings/avg price via patches.enrichment
- Uses earliest session date (patch already added separately)
"""

import asyncio
import logging
import os
import re
import sqlite3
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

from movie_scraper.patches.enrichment import enrich_film, build_description

LOG_LEVEL = os.getenv("MOVIE_SCRAPER_LOG_LEVEL", "DEBUG").upper()
USER_AGENT_BASE = os.getenv("MOVIE_SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36")
ACCEPT_LANG = os.getenv("MOVIE_SCRAPER_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
PROXY_URL = os.getenv("MOVIE_SCRAPER_PROXY_URL")
RATE_MIN = float(os.getenv("MOVIE_SCRAPER_RATE_LIMIT", "5.0"))
MAX_FILMS = int(os.getenv("MOVIE_SCRAPER_MAX_FILMS", "5"))
CACHE_TTL_DAYS = int(os.getenv("MOVIE_SCRAPER_CACHE_TTL_DAYS", "15"))

logger = logging.getLogger("movie_scraper.simple_scraper")
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")

BASE = "https://www.afisha.ru"
LIST_URL = f"{BASE}/prm/schedule_cinema/"
RAW_ICS_URL = "https://raw.githubusercontent.com/MaxYtre/movie/main/docs/calendar.ics"

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}

DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
CACHE_DB = DATA_DIR / "cache.sqlite"

class CacheDB:
    def __init__(self, path: Path) -> None:
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS films (
              slug TEXT PRIMARY KEY,
              title TEXT,
              country TEXT,
              rating TEXT,
              description TEXT,
              age TEXT,
              url TEXT,
              updated_at TEXT,
              imdb_rating REAL,
              kp_rating REAL,
              trailer_url TEXT,
              poster_url TEXT,
              year INTEGER
            );
            CREATE TABLE IF NOT EXISTS sessions (
              slug TEXT PRIMARY KEY,
              next_date TEXT,
              updated_at TEXT
            );
            """
        )
        self.conn.commit()

    def get_film_row(self, slug: str):
        cur = self.conn.execute("SELECT slug,title,country,rating,description,age,url,updated_at,imdb_rating,kp_rating,trailer_url,poster_url,year FROM films WHERE slug=?", (slug,))
        return cur.fetchone()

    def upsert_film(self, slug: str, title: Optional[str], country: Optional[str], rating: Optional[str], description: Optional[str], age: Optional[str], url: str,
                    imdb_rating: Optional[float]=None, kp_rating: Optional[float]=None, trailer_url: Optional[str]=None, poster_url: Optional[str]=None, year: Optional[int]=None) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "REPLACE INTO films(slug,title,country,rating,description,age,url,updated_at,imdb_rating,kp_rating,trailer_url,poster_url,year) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (slug, title, country, rating, description, age, url, now, imdb_rating, kp_rating, trailer_url, poster_url, year)
        )
        self.conn.commit()

    def get_session(self, slug: str) -> Optional[date]:
        cur = self.conn.execute("SELECT next_date FROM sessions WHERE slug=?", (slug,))
        row = cur.fetchone()
        if row and row[0]:
            try:
                return datetime.fromisoformat(row[0]).date()
            except Exception:
                return None
        return None

    def upsert_session(self, slug: str, next_date: Optional[date]) -> None:
        now = datetime.utcnow().isoformat()
        nd = next_date.isoformat() if next_date else None
        self.conn.execute("REPLACE INTO sessions(slug,next_date,updated_at) VALUES(?,?,?)", (slug, nd, now))
        self.conn.commit()

    def is_fresh(self, slug: str, ttl_days: int) -> bool:
        cur = self.conn.execute("SELECT updated_at FROM films WHERE slug=?", (slug,))
        row = cur.fetchone()
        if not row or not row[0]:
            return False
        try:
            ts = datetime.fromisoformat(row[0])
            return datetime.utcnow() - ts < timedelta(days=ttl_days)
        except Exception:
            return False

class Film:
    def __init__(self, title: str, url: str, slug: str):
        self.title = title
        self.url = url
        self.slug = slug
        self.country: Optional[str] = None
        self.age_limit: Optional[str] = None
        self.rating: Optional[str] = None
        self.description: Optional[str] = None
        self.next_date: Optional[date] = None
        # enrichment fields
        self.imdb_rating: Optional[float] = None
        self.kp_rating: Optional[float] = None
        self.trailer_url: Optional[str] = None
        self.poster_url: Optional[str] = None
        self.year: Optional[int] = None
        self.avg_price: Optional[int] = None

    @property
    def is_foreign(self) -> bool:
        if not self.country:
            return False
        for c in re.split(r"[,/;|]", self.country):
            if re.search(r"\b(россия|russia|рф|ссср|ussr)\b", c.strip(), re.IGNORECASE):
                return False
        return True

# ... fetch, robust_get, and parse_* functions unchanged except date parser patch can be applied separately ...

async def scrape() -> Tuple[List['Film'], dict]:
    # ... preamble unchanged ...
    processed: List[Film] = []
    db = CacheDB(CACHE_DB)
    for i, f in enumerate(films, 1):
        date_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
        stats["region"].append((f.slug, date_url))

        cached_ok = False
        if db.is_fresh(f.slug, CACHE_TTL_DAYS):
            row = db.get_film_row(f.slug)
            next_dt = db.get_session(f.slug)
            if row:
                (_, title, country, _, description, age, _, _, imdb_r, kp_r, trl, pst, year_val) = row
                f.title = title or f.title
                f.country = country
                f.description = description
                f.age_limit = age
                f.next_date = next_dt
                f.imdb_rating, f.kp_rating, f.trailer_url, f.poster_url, f.year = imdb_r, kp_r, trl, pst, year_val
                stats["cache_hits"] += 1
                stats["selectors"].append((f.slug, "cache", "cache", "cache", "cache" if next_dt else "cache-miss-date"))
                cached_ok = True

        if cached_ok:
            keep = bool(f.country) and f.is_foreign and bool(f.next_date)
            if not f.country: stats["reasons"].append((f.slug, "NO_COUNTRY"))
            elif not f.is_foreign: stats["reasons"].append((f.slug, "NOT_FOREIGN"))
            elif not f.next_date: stats["reasons"].append((f.slug, "NO_DATE"))
            if keep: processed.append(f)
            pause = RATE_MIN + random.uniform(0.1, 0.3)
            stats["sleep_total"] += pause
            await asyncio.sleep(pause)
            continue

        stats["cache_misses"] += 1

        html = await robust_get(session, urljoin(BASE, f"/movie/{f.slug}/"), stats["backoffs"])
        if not html:
            stats["reasons"].append((f.slug, "DETAIL_FAIL"))
            continue
        soup = BeautifulSoup(html, 'lxml')
        f.country, c_via = parse_country_new(soup)
        f.age_limit, a_via = parse_age_new(soup)
        f.description, d_via = parse_desc_new(soup)
        t_override = parse_item_name(soup)
        if t_override: f.title = t_override

        date_html = await robust_get(session, date_url, stats["backoffs"])
        f.next_date, n_via = (None, "miss")
        date_soup = None
        if date_html:
            date_soup = BeautifulSoup(date_html, 'lxml')
            f.next_date, n_via = parse_first_day_new(date_soup)

        # Enrichment (safe if APIs not set)
        try:
            if date_soup is None:
                date_soup = BeautifulSoup(date_html or "", 'lxml')
            await enrich_film(session, f, soup, date_soup)
        except Exception:
            pass

        stats["selectors"].append((f.slug, c_via, a_via, d_via, n_via))

        keep = bool(f.country) and f.is_foreign and bool(f.next_date)
        if not f.country: stats["reasons"].append((f.slug, "NO_COUNTRY"))
        elif not f.is_foreign: stats["reasons"].append((f.slug, "NOT_FOREIGN"))
        elif not f.next_date: stats["reasons"].append((f.slug, "NO_DATE"))

        db.upsert_film(f.slug, f.title, f.country, None, f.description, f.age_limit, f.url,
                       imdb_rating=f.imdb_rating, kp_rating=f.kp_rating, trailer_url=f.trailer_url, poster_url=f.poster_url, year=f.year)
        db.upsert_session(f.slug, f.next_date)
        if keep: processed.append(f)

        pause = RATE_MIN + random.uniform(0.3, 0.9)
        stats["sleep_total"] += pause
        await asyncio.sleep(pause)

    return processed, stats

# In write_ics(), replace description assembly with build_description
# ev.add('description', build_description(f))
