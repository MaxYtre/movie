"""
Enrichment module: Enhanced version with better IMDb search using original titles from Kinopoisk.dev
"""

import os, re, asyncio, json, time
from datetime import date
from typing import Optional, Tuple
from urllib.parse import urlencode, urljoin, quote_plus

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
    if diag is not None:
        diag.append(msg)

def clean_title_for_search(title: str) -> str:
    """
    Очищаем название от лишних символов для лучшего поиска
    """
    # Удаляем годы в скобках или после двоеточия
    title = re.sub(r'\s*[:(]\s*(19|20)\d{2}\s*[)]?.*$', '', title)
    
    # Удаляем субтитры типа "часть X", "эпизод X", римские цифры
    title = re.sub(r'\s*[:-]?\s*(часть|эпизод|episode|part)\s+[IVX\d]+.*$', '', title, flags=re.IGNORECASE)
    
    # Удаляем римские цифры в конце
    title = re.sub(r'\s+[IVX]+\s*$', '', title)
    
    # Удаляем лишние пробелы и знаки препинания
    title = re.sub(r'[.,!?;:]\s*$', '', title.strip())
    
    return title.strip()

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

async def get_original_title_from_kp(session, title: str, year: Optional[int], diag=None) -> Tuple[Optional[str], Optional[float]]:
    """
    Получаем оригинальное название и рейтинг Кинопоиска одним запросом
    Возвращает (original_title, kp_rating)
    """
    if not KINOPOISK_API_KEY:
        return None, None
    
    headers = {'X-API-KEY': KINOPOISK_API_KEY}
    q = quote_plus(clean_title_for_search(title))
    
    # Используем более точные поля для поиска
    fields = "id,name,alternativeName,year,rating.kp,poster"
    url = f"https://api.kinopoisk.dev/v1.4/movie/search?page=1&limit=5&query={q}&selectFields={fields}"
    
    status, data, dt = await fetch_json(session, url, headers=headers)
    await asyncio.sleep(API_COOLDOWN)
    
    original_title = None
    kp_rating = None
    
    try:
        docs = data and data.get('docs') or []
        
        # Ищем наиболее подходящий результат
        best_match = None
        for doc in docs:
            # Проверяем совпадение по году, если известен
            if year and doc.get('year'):
                year_diff = abs(doc['year'] - year)
                if year_diff <= 1:  # Допускаем погрешность в 1 год
                    best_match = doc
                    break
        
        # Если не нашли по году, берем первый результат
        if not best_match and docs:
            best_match = docs[0]
        
        if best_match:
            # Получаем оригинальное название
            original_title = best_match.get('alternativeName') or best_match.get('name')
            
            # Получаем рейтинг Кинопоиска
            rating_obj = best_match.get('rating') or {}
            kp = rating_obj.get('kp')
            if kp and kp not in ('null', 'N/A', '—'):
                try:
                    kp_rating = float(kp)
                except (ValueError, TypeError):
                    pass
    
    except Exception as e:
        _log(diag, f"[KP] error parsing response: {e}")
    
    _log(diag, f"[KP.dev] title='{title}' year={year or '-'} status={status} original='{original_title or '-'}' kp={kp_rating or '-'} t={dt:.2f}s")
    return original_title, kp_rating

async def get_imdb_rating_enhanced(session, title: str, year: Optional[int], original_title: Optional[str], diag=None):
    """
    Улучшенный поиск рейтинга IMDb с каскадным поиском
    """
    if not OMDB_API_KEY:
        return None
    
    # Список названий для поиска в порядке приоритета
    search_titles = []
    
    # 1. Оригинальное название из Кинопоиска (если есть)
    if original_title:
        search_titles.append(clean_title_for_search(original_title))
    
    # 2. Очищенное русское название
    cleaned_ru = clean_title_for_search(title)
    if cleaned_ru not in search_titles:
        search_titles.append(cleaned_ru)
    
    # 3. Оригинальное название без года (на случай если в оригинальном есть год)
    if original_title:
        cleaned_original = clean_title_for_search(original_title)
        if cleaned_original != original_title and cleaned_original not in search_titles:
            search_titles.append(cleaned_original)
    
    for i, search_title in enumerate(search_titles):
        params = urlencode({
            'apikey': OMDB_API_KEY, 
            't': search_title,
            **(({'y': year} if year else {}))
        })
        url = f"http://www.omdbapi.com/?{params}"
        status, data, dt = await fetch_json(session, url)
        
        rating = None
        found = False
        
        try:
            if data and data.get('Response') == 'True':
                r = data.get('imdbRating')
                if r and r != 'N/A':
                    rating = float(r)
                    found = True
        except Exception:
            rating = None
        
        attempt_info = f"attempt={i+1}/{len(search_titles)} title='{search_title}' year={year or '-'} status={status} imdb={rating or '-'} t={dt:.2f}s"
        _log(diag, f"[OMDb] {attempt_info}")
        
        # Если нашли рейтинг, прерываем поиск
        if found:
            _log(diag, f"[OMDb] SUCCESS with {search_title}")
            await asyncio.sleep(API_COOLDOWN)
            return rating
        
        await asyncio.sleep(API_COOLDOWN)
    
    _log(diag, f"[OMDb] NO RATING FOUND for any variant of '{title}'")
    return None

async def find_trailer_youtube(session, title: str, year: Optional[int], original_title: Optional[str] = None, diag=None):
    if not YOUTUBE_API_KEY:
        return None
    
    # Пробуем искать по оригинальному названию, если есть
    search_title = original_title if original_title else title
    q = f"{search_title} trailer"
    if year: 
        q += f" {year}"
    
    params = urlencode({
        'part': 'snippet',
        'q': q,
        'type': 'video',
        'maxResults': 1,
        'key': YOUTUBE_API_KEY,
        'safeSearch': 'none'
    })
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"
    status, data, dt = await fetch_json(session, url)
    await asyncio.sleep(API_COOLDOWN)
    
    vid = None
    if data and data.get('items'):
        vid = data['items'][0]['id'].get('videoId')
    
    _log(diag, f"[API] YT q='{q}' status={status} videoId={vid or '-'} t={dt:.2f}s")
    return f"https://www.youtube.com/watch?v={vid}" if vid else None

# Оставляем старые функции для обратной совместимости
async def get_imdb_rating(session, title: str, year: Optional[int], diag=None):
    """Устаревшая функция - используется для обратной совместимости"""
    return await get_imdb_rating_enhanced(session, title, year, None, diag)

async def get_kp_rating(session, title: str, diag=None):
    """Устаревшая функция - получение только рейтинга КП"""
    _, rating = await get_original_title_from_kp(session, title, None, diag)
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
    # Постер
    film.poster_url = parse_poster(detail_soup)
    _log(diag, f"[POSTER] url={film.poster_url or '-'}")
    
    # Год
    film.year = parse_year(detail_soup)
    
    # Трейлер с афиши
    afisha_tr = detail_soup.select_one('video[src]')
    film.trailer_url = (afisha_tr and afisha_tr.get('src')) or None
    _log(diag, f"[TRAILER] afisha={'1' if film.trailer_url else '0'} url={film.trailer_url or '-'}")
    
    # Получаем оригинальное название и рейтинг КП одним запросом
    original_title, kp_rating = await get_original_title_from_kp(session, film.title, film.year, diag)
    film.kp_rating = kp_rating
    
    # Улучшенный поиск рейтинга IMDb с использованием оригинального названия
    film.imdb_rating = await get_imdb_rating_enhanced(session, film.title, film.year, original_title, diag)
    
    # Трейлер на YouTube (если не нашли на афише)
    if not film.trailer_url:
        film.trailer_url = await find_trailer_youtube(session, film.title, film.year, original_title, diag)
    
    # Средняя цена
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