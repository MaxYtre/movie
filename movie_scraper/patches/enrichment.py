"""
Add ratings (IMDb/Kinopoisk via APIs), average price parsing, trailer and poster links.
Stores new fields in films table; uses env API keys. Safe rate limiting and caching.
"""

import os, re, asyncio, json
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urlencode, urljoin

import aiohttp
from bs4 import BeautifulSoup

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KINOPOISK_API_KEY = os.getenv("KINOPOISK_API_KEY")
API_COOLDOWN = float(os.getenv("API_COOLDOWN", "10.0"))  # seconds between external API calls
BASE = "https://www.afisha.ru"

# ---- DB helpers patch (to be merged into CacheDB) ----
DDL = """
ALTER TABLE films ADD COLUMN imdb_rating REAL;
ALTER TABLE films ADD COLUMN kp_rating REAL;
ALTER TABLE films ADD COLUMN trailer_url TEXT;
ALTER TABLE films ADD COLUMN poster_url TEXT;
ALTER TABLE films ADD COLUMN year INTEGER;
"""

async def ensure_columns(db_conn):
    # Add columns if not exist (best-effort)
    cur = db_conn.execute("PRAGMA table_info(films)")
    cols = {row[1] for row in cur.fetchall()}
    missing = []
    if 'imdb_rating' not in cols: missing.append('imdb_rating REAL')
    if 'kp_rating' not in cols: missing.append('kp_rating REAL')
    if 'trailer_url' not in cols: missing.append('trailer_url TEXT')
    if 'poster_url' not in cols: missing.append('poster_url TEXT')
    if 'year' not in cols: missing.append('year INTEGER')
    if missing:
        db_conn.execute(f"ALTER TABLE films ADD COLUMN {missing[0]}")
        for col in missing[1:]:
            db_conn.execute(f"ALTER TABLE films ADD COLUMN {col}")
        db_conn.commit()

# ---- Parsers ----

def parse_poster(soup: BeautifulSoup) -> Optional[str]:
    img = soup.select_one('img[alt*="постер" i], img[alt*="poster" i], [class*="poster"] img, img[data-src]')
    if img:
        return img.get('src') or img.get('data-src')
    # hero video poster attribute
    video = soup.select_one('video[poster]')
    if video:
        return video.get('poster')
    return None

async def fetch_json(session, url, headers=None):
    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            return None
        try:
            return await resp.json()
        except Exception:
            return None

async def find_trailer_youtube(session, title: str, year: Optional[int]):
    if not YOUTUBE_API_KEY:
        return None
    q = f"{title} трейлер"
    if year: q += f" {year}"
    params = urlencode({
        'part': 'snippet',
        'q': q,
        'type': 'video',
        'maxResults': 1,
        'key': YOUTUBE_API_KEY,
        'safeSearch': 'none'
    })
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"
    data = await fetch_json(session, url)
    await asyncio.sleep(API_COOLDOWN)
    if not data or not data.get('items'):
        return None
    vid = data['items'][0]['id'].get('videoId')
    return f"https://www.youtube.com/watch?v={vid}" if vid else None

async def get_imdb_rating(session, title: str, year: Optional[int]):
    if not OMDB_API_KEY:
        return None
    params = urlencode({'apikey': OMDB_API_KEY, 't': title, **({'y': year} if year else {})})
    url = f"http://www.omdbapi.com/?{params}"
    data = await fetch_json(session, url)
    await asyncio.sleep(API_COOLDOWN)
    try:
        rating = data and data.get('imdbRating')
        return float(rating) if rating and rating != 'N/A' else None
    except Exception:
        return None

async def get_kp_rating(session, title: str):
    if not KINOPOISK_API_KEY:
        return None
    headers = { 'X-API-KEY': KINOPOISK_API_KEY }
    url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword?keyword={title}"
    data = await fetch_json(session, url, headers=headers)
    await asyncio.sleep(API_COOLDOWN)
    try:
        films = data and data.get('films') or []
        if not films:
            return None
        rating = films[0].get('rating')
        return float(rating) if rating not in (None, 'null', 'N/A', '—') else None
    except Exception:
        return None

def parse_year(soup: BeautifulSoup) -> Optional[int]:
    meta = soup.get_text(" ", strip=True)
    m = re.search(r"\b(19|20)\d{2}\b", meta)
    return int(m.group(0)) if m else None

def parse_prices(soup: BeautifulSoup) -> Optional[int]:
    prices = []
    for el in soup.select('a,span,div'):
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        m = re.search(r"(\d{2,5})\s*₽", txt)
        if m:
            try:
                val = int(m.group(1))
                if 50 <= val <= 5000:
                    prices.append(val)
            except ValueError:
                pass
    return int(sum(prices)/len(prices)) if prices else None

async def enrich_film(session: aiohttp.ClientSession, film, detail_soup: BeautifulSoup, date_soup: BeautifulSoup):
    # Poster
    film.poster_url = parse_poster(detail_soup)
    # Year
    film.year = parse_year(detail_soup)
    # Trailer: prefer on-page video
    vid = detail_soup.select_one('video[src]')
    film.trailer_url = (vid and vid.get('src')) or None
    if not film.trailer_url:
        film.trailer_url = await find_trailer_youtube(session, film.title, film.year)
    # Ratings
    film.imdb_rating = await get_imdb_rating(session, film.title, film.year)
    film.kp_rating = await get_kp_rating(session, film.title)
    # Average price
    film.avg_price = parse_prices(date_soup)
    return film

# ---- ICS extension (snippet idea) ----

def build_description(f):
    parts = []
    if f.age_limit: parts.append(f.age_limit)
    if f.country: parts.append("Страна " + f.country)
    if f.imdb_rating: parts.append(f"IMDb: {f.imdb_rating:.1f}")
    if f.kp_rating: parts.append(f"Кинопоиск: {f.kp_rating:.1f}")
    if f.avg_price: parts.append(f"Средняя цена: {f.avg_price} ₽")
    if f.description: parts.append("\n" + f.description)
    if f.trailer_url: parts.append("\nТрейлер: " + f.trailer_url)
    if f.poster_url: parts.append("\nПостер: " + f.poster_url)
    more_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
    parts.append("\nПодробнее: " + more_url)
    return "\n".join(parts)
