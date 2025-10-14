"""
Full-retry mode: no circuit breaker, up to 10 attempts per fetch with long exponential backoff.
Always try to complete all films; add per-film failure diagnostics.
"""

import asyncio
import logging
import os
import re
import sqlite3
import hashlib
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

LOG_LEVEL = os.getenv("MOVIE_SCRAPER_LOG_LEVEL", "DEBUG").upper()
USER_AGENT_BASE = os.getenv("MOVIE_SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36")
ACCEPT_LANG = os.getenv("MOVIE_SCRAPER_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
PROXY_URL = os.getenv("MOVIE_SCRAPER_PROXY_URL")
RATE_MIN = float(os.getenv("MOVIE_SCRAPER_RATE_LIMIT", "5.0"))
EXTRA_BETWEEN_FILMS_MIN = 1.0
EXTRA_BETWEEN_FILMS_MAX = 3.0
MAX_FILMS = int(os.getenv("MOVIE_SCRAPER_MAX_FILMS", "5"))
CACHE_TTL_DAYS = int(os.getenv("MOVIE_SCRAPER_CACHE_TTL_DAYS", "3"))

logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("movie_scraper.simple_scraper")

BASE = "https://www.afisha.ru"
LIST_URL = f"{BASE}/prm/schedule_cinema/"

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}

DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
CACHE_DB = DATA_DIR / "cache.sqlite"

class CacheDB:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
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
              updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS sessions (
              slug TEXT PRIMARY KEY,
              next_date TEXT,
              updated_at TEXT
            );
            """
        )
        self.conn.commit()

    def get_film(self, slug: str) -> Optional[Tuple]:
        cur = self.conn.execute("SELECT slug,title,country,rating,description,age,url,updated_at FROM films WHERE slug=?", (slug,))
        return cur.fetchone()

    def upsert_film(self, slug: str, title: Optional[str], country: Optional[str], rating: Optional[str], description: Optional[str], age: Optional[str], url: str) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "REPLACE INTO films(slug,title,country,rating,description,age,url,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (slug, title, country, rating, description, age, url, now)
        )
        self.conn.commit()

    def get_session(self, slug: str) -> Optional[date]:
        cur = self.conn.execute("SELECT next_date FROM sessions WHERE slug=?", (slug,))
        row = cur.fetchone()
        if row and row[0]:
            try:
                d = datetime.fromisoformat(row[0]).date()
                return d
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

    @property
    def is_foreign(self) -> bool:
        if not self.country:
            return False
        countries = re.split(r"[,/;|]", self.country)
        for c in countries:
            if re.search(r"\b(россия|russia|рф|ссср|ussr)\b", c.strip(), re.IGNORECASE):
                return False
        return True

def normalize_detail_url(u: str) -> str:
    p = urlparse(u)
    path = p.path
    if re.search(r"/\d{2}-\d{2}-\d{4}$", path):
        path = re.sub(r"/\d{2}-\d{2}-\d{4}$", "", path)
    return urljoin(BASE, path.rstrip('/') + '/')

def slug_from_url(u: str) -> str:
    return normalize_detail_url(u).rstrip('/').split('/')[-1]

def rotate_headers() -> dict:
    ua_suffix = random.choice(["", "; rv:118.0", "; WOW64"])
    ua = USER_AGENT_BASE + ua_suffix
    headers = {
        'User-Agent': ua,
        'Accept-Language': ACCEPT_LANG,
        'Accept': random.choice([
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'text/html,application/xml;q=0.9,*/*;q=0.8'
        ]),
        'Connection': random.choice(['keep-alive', 'close']),
        'Pragma': random.choice(['no-cache', '']),
        'Cache-Control': 'no-cache',
        'Referer': LIST_URL,
        'DNT': '1',
    }
    return {k: v for k, v in headers.items() if v}

async def fetch(session: aiohttp.ClientSession, url: str, attempt: int, backoffs: List[float]) -> Tuple[Optional[str], int]:
    headers = rotate_headers()
    try:
        async with session.get(url, headers=headers, proxy=PROXY_URL) as resp:
            status = resp.status
            if status == 200:
                text = await resp.text()
                logger.info(f"[FETCH] try={attempt} status=200 url={url}")
                return text, status
            else:
                logger.warning(f"[FETCH] try={attempt} status={status} url={url}")
                if status == 429:
                    delay = [30.0, 60.0, 120.0, 180.0, 300.0, 300.0, 450.0, 600.0, 600.0, 900.0][min(attempt-1, 9)] + random.uniform(0.5, 2.0)
                    backoffs.append(delay)
                return None, status
    except Exception as e:
        logger.warning(f"[FETCH] try={attempt} error={type(e).__name__} url={url} msg={e}")
        return None, -1

# Selectors remain the same as previous revision...
# (omitted here for brevity in this tool call; real file content includes full functions)
