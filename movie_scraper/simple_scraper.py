"""
Afisha (Perm) scraper with enrichment integration (development branch).
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
from movie_scraper.patches.parse_first_day_new_fix import parse_first_day_new
from movie_scraper.patches.migration import ensure_enrichment_columns

LOG_LEVEL = os.getenv("MOVIE_SCRAPER_LOG_LEVEL", "DEBUG").upper()
USER_AGENT_BASE = os.getenv("MOVIE_SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36")
ACCEPT_LANG = os.getenv("MOVIE_SCRAPER_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
PROXY_URL = os.getenv("MOVIE_SCRAPER_PROXY_URL")
RATE_MIN = float(os.getenv("MOVIE_SCRAPER_RATE_LIMIT", "5.0"))
MAX_FILMS = int(os.getenv("MOVIE_SCRAPER_MAX_FILMS", "50"))
CACHE_TTL_DAYS = int(os.getenv("MOVIE_SCRAPER_CACHE_TTL_DAYS", "15"))

logger = logging.getLogger("movie_scraper.simple_scraper")
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")

BASE = "https://www.afisha.ru"
LIST_URL = f"{BASE}/prm/schedule_cinema/"
RAW_ICS_URL = "https://raw.githubusercontent.com/MaxYtre/movie/development/docs/calendar.ics"

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

def parse_item_name(soup: BeautifulSoup) -> Optional[str]:
    name = soup.select_one('[data-test="ITEM-NAME"]')
    if name:
        return name.get_text(" ", strip=True)
    h = soup.find('h1')
    if h:
        return h.get_text(" ", strip=True)
    og = soup.find('meta', attrs={'property': 'og:title'})
    if og and og.get('content'):
        return og['content'].strip()
    return None

async def fetch(session: aiohttp.ClientSession, url: str, attempt: int, backoffs: List[float]) -> Tuple[Optional[str], int]:
    headers = {
        'User-Agent': USER_AGENT_BASE,
        'Accept-Language': ACCEPT_LANG,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': LIST_URL,
    }
    try:
        async with session.get(url, headers=headers, proxy=PROXY_URL) as resp:
            status = resp.status
            if status == 200:
                return await resp.text(), status
            if status == 429:
                delay = [30.0, 60.0, 120.0, 180.0, 300.0, 300.0, 450.0, 600.0, 600.0, 900.0][min(attempt-1, 9)]
                backoffs.append(delay)
            return None, status
    except Exception:
        return None, -1

async def robust_get(session: aiohttp.ClientSession, url: str, backoffs: List[float]) -> Optional[str]:
    for attempt in range(1, 11):
        html, status = await fetch(session, url, attempt, backoffs)
        if html:
            return html
        await asyncio.sleep([30.0, 60.0, 120.0, 180.0, 300.0, 300.0, 450.0, 600.0, 600.0, 900.0][min(attempt-1, 9)])
    return None

def parse_country_new(soup: BeautifulSoup) -> Tuple[Optional[str], str]:
    el = soup.select_one('[data-test="ITEM-META"] a[href*="/movie/strana-"]')
    if el:
        return el.get_text(" ", strip=True), "item-meta"
    meta = soup.select_one('[data-test="ITEM-META"]')
    if meta:
        txt = meta.get_text(" ", strip=True)
        m = re.search(r"([A-Za-zА-Яа-яЁё\-\s]+)\s*,\s*\d{4}", txt)
        if m:
            return m.group(1).strip(), "item-meta-text"
    return None, "miss"

def parse_age_new(soup: BeautifulSoup) -> Tuple[Optional[str], str]:
    el = soup.select_one('tr[aria-label="Возраст"] [data-test="META-FIELD-VALUE"]')
    if el:
        return el.get_text(" ", strip=True), "table"
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"\b(\d{1,2}\+)\b", txt)
    return (m.group(1) if m else None), ("regex" if m else "miss")

def parse_desc_new(soup: BeautifulSoup) -> Tuple[Optional[str], str]:
    el = soup.select_one('[data-test="OBJECT-DESCRIPTION-CONTENT"]')
    if el:
        desc = el.get_text(" ", strip=True)
        return (desc[:300] + '...') if len(desc) > 300 else desc, "object-desc"
    return None, "miss"

async def scrape() -> Tuple[List['Film'], dict]:
    stats = {"429": 0, "403": 0, "errors": 0, "cache_hits": 0, "cache_misses": 0, "sleep_total": 0.0, "backoffs": [], "selectors": [], "region": [], "reasons": []}
    logger.info(f"[BOOT] py={os.sys.version.split()[0]} ua={USER_AGENT_BASE[:20]}… proxy={'on' if PROXY_URL else 'off'}")
    films: List[Film] = []
    timeout = aiohttp.ClientTimeout(total=3600)
    connector = aiohttp.TCPConnector(limit=6)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        page = 1
        while page <= 10 and len(films) < MAX_FILMS:
            html = await robust_get(session, LIST_URL if page == 1 else f"{LIST_URL}page{page}/", stats["backoffs"])
            if not html:
                break
            soup = BeautifulSoup(html, 'lxml')
            for it in soup.select('div[data-test="ITEM"]'):
                if len(films) >= MAX_FILMS:
                    break
                link = it.select_one('a[data-test="LINK ITEM-URL"]')
                if not link or not link.get('href'):
                    continue
                href = urlparse(link['href'])._replace(query='', fragment='').geturl()
                detail = urljoin(BASE, href)
                s = slug_from_url(detail)
                name_link = it.select_one('[data-test="LINK ITEM-NAME ITEM-URL"]')
                title = name_link.get_text(" ", strip=True) if name_link else s
                films.append(Film(title=title, url=detail, slug=s))
            page += 1
            pause = RATE_MIN + random.uniform(0.3, 0.9)
            stats["sleep_total"] += pause
            await asyncio.sleep(pause)

        processed: List[Film] = []
        db = CacheDB(CACHE_DB)
        ensure_enrichment_columns(db.conn)
        for i, f in enumerate(films, 1):
            date_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
            stats["region"].append((f.slug, date_url))

            cached_ok = False
            if CACHE_TTL_DAYS > 0 and db.is_fresh(f.slug, CACHE_TTL_DAYS):
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

            try:
                if date_soup is None:
                    date_soup = BeautifulSoup(date_html or "", 'lxml')
                await enrich_film(session, f, soup, date_soup, diag=stats.setdefault('diag_ext', []))
            except Exception as e:
                stats.setdefault('diag_ext', []).append(f"[ENRICH] error slug={f.slug} {type(e).__name__}: {e}")

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

def slug_from_url(u: str) -> str:
    return urlparse(u).path.rstrip('/').split('/')[-1]

def write_ics(films: List[Film], docs_dir: Path) -> Path:
    cal = Calendar()
    cal.add('prodid', '-//Perm Foreign Films//perm-cinema//EN')
    cal.add('version', '2.0')
    cal.add('calscale', 'GREGORIAN')
    cal.add('method', 'PUBLISH')
    cal.add('x-wr-calname', 'Зарубежные фильмы в кинотеатрах Перми')
    cal.add('x-wr-caldesc', 'Иностранные фильмы, идущие в кинотеатрах Перми. Обновляется ежедневно.')
    for f in films:
        ev = Event()
        dt = f.next_date or date.today()
        ev.add('uid', f"{f.slug}-{dt.isoformat()}@perm-cinema")
        ev.add('dtstart', dt); ev['dtstart'].params['VALUE'] = 'DATE'
        ev.add('dtend', dt);   ev['dtend'].params['VALUE'] = 'DATE'
        ev.add('dtstamp', datetime.utcnow())
        title = ("🎬 " + f.title)
        ev.add('summary', title)
        ev.add('description', build_description(f))
        more_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
        ev.add('url', more_url)
        ev.add('categories', ['ЗАРУБЕЖНЫЕ-ФИЛЬМЫ','КИНО','ПЕРМЬ'])
        cal.add_component(ev)
    docs_dir.mkdir(exist_ok=True)
    ics_path = docs_dir / "calendar.ics"
    payload = cal.to_ical()
    with open(ics_path, "wb") as fh: fh.write(payload)
    logger.info(f"[ICS] events={len(films)} size={len(payload)} path={ics_path}")
    return ics_path

def write_index(docs_dir: Path, films_count: int, preview: List[str], stats: dict):
    preview_html = "".join(f"<li>{p}</li>" for p in preview)
    region_html = "".join(f"<li>{slug}: {url}</li>" for slug, url in stats.get('region', [])[:10])
    sel_html = "".join(f"<li>{slug}: country={c}, age={a}, desc={d}, date={n}</li>" for slug, c, a, d, n in stats.get('selectors', [])[:10])
    # Extended DIAG block (API logs)
    diag_ext = stats.get('diag_ext', [])
    diag_ext_html = "".join(f"<li>{line}</li>" for line in diag_ext[:50])
    reasons_html = "".join(f"<li>{slug}: {reason}</li>" for slug, reason in stats.get('reasons', [])[:20])
    html = (
        f"<!doctype html><html lang=\"ru\"><meta charset=\"utf-8\"><title>Календарь фильмов</title>\n"
        f"<body style=\"font-family:Arial,sans-serif;max-width:800px;margin:20px auto;\">\n"
        f"<h1>Календарь зарубежных фильмов (Пермь)</h1>\n"
        f"<p>Фильм(ов) в календаре: <strong>{films_count}</strong></p>\n"
        f"<p><a href=\"{RAW_ICS_URL}\">Скачать календарь (.ics)</a></p>\n"
        f"<h3>Пример событий</h3><ul>{preview_html}</ul>\n"
        f"<details><summary>REGION date URLs</summary><ul>{region_html}</ul></details>\n"
        f"<details><summary>Новые селекторы</summary><ul>{sel_html}</ul></details>\n"
        f"<details><summary>API/Парсинг</summary><ul>{diag_ext_html}</ul></details>\n"
        f"<details><summary>Причины исключений</summary><ul>{reasons_html}</ul></details>\n"
        f"<hr>\n"
        f"<pre id=\"diag\" style=\"background:#f7f7f7;padding:10px;border:1px solid #ddd;white-space:pre-wrap;\"></pre>\n"
        f"<script>fetch('diag.txt').then(r=>r.text()).then(t=>document.getElementById('diag').textContent=t).catch(()=>{{}});</script>\n"
        f"</body></html>\n"
    )
    with open(docs_dir / "index.html", "w", encoding="utf-8") as f:
        f.write(html)

def write_diag(docs_dir: Path, lines: List[str]):
    with open(docs_dir / "diag.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

async def main():
    docs = Path("docs"); docs.mkdir(exist_ok=True)
    diag: List[str] = []
    try:
        diag.append(f"BOOT py={os.sys.version.split()[0]} ua={USER_AGENT_BASE[:30]}… proxy={'on' if PROXY_URL else 'off'}")
        films, stats = await scrape()
        diag.append(f"SUMMARY candidates_processed={len(films)}")
        preview = []
        for f in films[:10]:
            preview.append(f"{f.title} | {f.next_date} | {f.country} | {f.age_limit or ''} | {urljoin(BASE, f'/prm/schedule_cinema_product/{f.slug}/')}")
        ics_path = write_ics(films, docs)
        write_index(docs, len(films), preview, stats)
        diag.append("=== DIAG COPY START ===")
        diag.append(f"limit={MAX_FILMS} foreign_films={len(films)} 429={stats['429']} 403={stats['403']} cache_hits={stats['cache_hits']} cache_misses={stats['cache_misses']} sleep_total={stats['sleep_total']:.1f}")
        if stats['backoffs']:
            diag.append("429_backoffs=" + ",".join(f"{d:.1f}s" for d in stats['backoffs']))
        for slug, url in stats.get('region', [])[:10]: diag.append(f"REGION {slug} date_url={url}")
        for slug, c,a,d,n in stats.get('selectors', [])[:10]: diag.append(f"NEW-SELECTORS {slug} country={c} age={a} desc={d} date={n}")
        for slug, why in stats.get('reasons', [])[:20]: diag.append(f"REASON {slug} {why}")
        for p in preview: diag.append("PREVIEW " + p)
        diag.extend(stats.get('diag_ext', [])[:200])
        diag.append(f"ICS path={ics_path} exists={ics_path.exists()} size={ics_path.stat().st_size if ics_path.exists() else 0}")
        diag.append("=== DIAG COPY END ===")
    except Exception as e:
        diag.append(f"FAIL AT=main exception={type(e).__name__} msg={e}")
    finally:
        write_diag(docs, diag)
        logger.info("\n" + "\n".join(diag))

if __name__ == "__main__":
    asyncio.run(main())
