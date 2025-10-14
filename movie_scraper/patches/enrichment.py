"""
Enrichment module: add detailed DIAG logging for API calls, prices, poster, trailer.
"""

import os, re, asyncio, json, time
from datetime import date
from typing import Optional
from urllib.parse import urlencode, urljoin

import aiohttp
from bs4 import BeautifulSoup

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KINOPOISK_API_KEY = os.getenv("KINOPOISK_API_KEY")
API_COOLDOWN = float(os.getenv("API_COOLDOWN", "10.0"))
BASE = "https://www.afisha.ru"

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}

def _log(diag: list, msg: str):
    # push into diag list if provided
    if diag is not None:
        diag.append(msg)

def parse_poster(soup: BeautifulSoup) -> Optional[str]:
    img = soup.select_one('img[alt*="постер" i], img[alt*="poster" i], [class*="poster"] img, img[data-src]')
    if img:
        return img.get('src') or img.get('data-src')
    video = soup.select_one('video[poster]')
    if video:
        return video.get('poster')
    return None

async def fetch_json(session, url, headers=None):
    t0 = time.perf_counter()
    try:
        async with session.get(url, headers=headers) as resp:
            status = resp.status
            data = None
            try:
                data = await resp.json()
            except Exception:
                data = None
            dt = time.perf_counter() - t0
            return status, data, dt
    except Exception:
        dt = time.perf_counter() - t0
        return -1, None, dt

async def find_trailer_youtube(session, title: str, year: Optional[int], diag=None):
    if not YOUTUBE_API_KEY:
        return None
    q = f"{title} трейлер"
    if year: q += f" {year}"
    params = urlencode({'part':'snippet','q':q,'type':'video','maxResults':1,'key':YOUTUBE_API_KEY,'safeSearch':'none'})
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"
    status, data, dt = await fetch_json(session, url)
    await asyncio.sleep(API_COOLDOWN)
    vid = None
    if data and data.get('items'):
        vid = data['items'][0]['id'].get('videoId')
    _log(diag, f"[API] YT q='{q}' status={status} videoId={vid or '-'} t={dt:.2f}s")
    return f"https://www.youtube.com/watch?v={vid}" if vid else None

async def get_imdb_rating(session, title: str, year: Optional[int], diag=None):
    if not OMDB_API_KEY:
        return None
    params = urlencode({'apikey': OMDB_API_KEY, 't': title, **({'y': year} if year else {})})
    url = f"http://www.omdbapi.com/?{params}"
    status, data, dt = await fetch_json(session, url)
    await asyncio.sleep(API_COOLDOWN)
    rating = None
    try:
        r = data and data.get('imdbRating')
        rating = float(r) if r and r != 'N/A' else None
    except Exception:
        rating = None
    _log(diag, f"[API] OMDb title='{title}' year={year or '-'} status={status} imdb={rating or '-'} t={dt:.2f}s")
    return rating

async def get_kp_rating(session, title: str, diag=None):
    if not KINOPOISK_API_KEY:
        return None
    headers = { 'X-API-KEY': KINOPOISK_API_KEY }
    url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword?keyword={title}"
    status, data, dt = await fetch_json(session, url, headers=headers)
    await asyncio.sleep(API_COOLDOWN)
    rating = None
    try:
        films = data and data.get('films') or []
        if films:
            r = films[0].get('rating')
            rating = float(r) if r not in (None, 'null', 'N/A', '—') else None
    except Exception:
        rating = None
    _log(diag, f"[API] KP title='{title}' status={status} kp={rating or '-'} t={dt:.2f}s")
    return rating

def parse_year(soup: BeautifulSoup) -> Optional[int]:
    meta = soup.get_text(" ", strip=True)
    m = re.search(r"\b(19|20)\d{2}\b", meta)
    return int(m.group(0)) if m else None

def parse_prices(soup: BeautifulSoup, diag=None) -> Optional[int]:
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
    avg = int(sum(prices)/len(prices)) if prices else None
    _log(diag, f"[PRICE] found={len(prices)} avg={avg or '-'}")
    return avg

async def enrich_film(session: aiohttp.ClientSession, film, detail_soup: BeautifulSoup, date_soup: BeautifulSoup, diag: list=None):
    film.poster_url = parse_poster(detail_soup)
    _log(diag, f"[POSTER] url={film.poster_url or '-'}")
    film.year = parse_year(detail_soup)
    afisha_tr = detail_soup.select_one('video[src]')
    film.trailer_url = (afisha_tr and afisha_tr.get('src')) or None
    _log(diag, f"[TRAILER] afisha={'1' if film.trailer_url else '0'} url={film.trailer_url or '-'}")
    if not film.trailer_url:
        film.trailer_url = await find_trailer_youtube(session, film.title, film.year, diag)
    film.imdb_rating = await get_imdb_rating(session, film.title, film.year, diag)
    film.kp_rating = await get_kp_rating(session, film.title, diag)
    film.avg_price = parse_prices(date_soup, diag)
    return film

from urllib.parse import urljoin as _u

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
    more_url = _u(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
    parts.append("\nПодробнее: " + more_url)
    return "\n".join(parts)
