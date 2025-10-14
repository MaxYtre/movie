"""
Final touches:
- Use film title from /movie/<slug>/ (ITEM-NAME) for ICS summary, not from schedule page.
- Add REGION line to DIAG.
- Keep region-specific date URL (/prm/...).
- Optionally raise MAX_FILMS via env later; keep defaults polite.
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
                    delay = [30.0, 60.0, 120.0][min(attempt-1, 2)] + random.uniform(0.5, 1.5)
                    backoffs.append(delay)
                return None, status
    except Exception as e:
        logger.warning(f"[FETCH] try={attempt} error={type(e).__name__} url={url} msg={e}")
        return None, -1


# New selectors for Afisha

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


def parse_first_day_new(soup: BeautifulSoup) -> Tuple[Optional[date], str]:
    day = soup.select_one('a[data-test="DAY"]:not([disabled])')
    if day and day.has_attr('aria-label'):
        label = day['aria-label'].strip().lower()
        m = re.match(r"(\d{1,2})\s+([а-я]+)", label)
        if m:
            d = int(m.group(1)); mon_name = m.group(2)
            mon = MONTHS_RU.get(mon_name)
            if mon:
                today = date.today()
                try:
                    cand = date(today.year, mon, d)
                    return cand, "calendar"
                except Exception:
                    pass
    return parse_next_date(soup), "fallback"


def parse_item_name(soup: BeautifulSoup) -> Optional[str]:
    # Prefer explicit ITEM-NAME if present
    name = soup.select_one('[data-test="ITEM-NAME"]')
    if name:
        return name.get_text(" ", strip=True)
    # fallback to <h1>
    h = soup.find('h1')
    if h:
        return h.get_text(" ", strip=True)
    return None


# Old date fallback

def parse_next_date(soup: BeautifulSoup) -> Optional[date]:
    today = date.today()
    txt = soup.get_text(" ", strip=True)
    dates: List[date] = []
    for m in re.finditer(r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)", txt, re.IGNORECASE):
        d = int(m.group(1)); mon = MONTHS_RU[m.group(2).lower()]
        try:
            cand = date(today.year, mon, d)
            if cand >= today:
                dates.append(cand)
        except ValueError:
            pass
    for m in re.finditer(r"(\d{2})\.(\d{2})\.(\d{4})", txt):
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            cand = date(y, mon, d)
            if cand >= today:
                dates.append(cand)
        except ValueError:
            pass
    if dates:
        return min(dates)
    return None


async def scrape() -> Tuple[List['Film'], dict]:
    stats = {"429": 0, "403": 0, "errors": 0, "cache_hits": 0, "cache_misses": 0, "sleep_total": 0.0, "backoffs": [], "miss_samples": [], "selectors": [], "region": []}
    logger.info(f"[BOOT] py={os.sys.version.split()[0]} ua={USER_AGENT_BASE[:20]}… proxy={'on' if PROXY_URL else 'off'}")
    films: List[Film] = []
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=8)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        page = 1
        while page <= 10 and len(films) < MAX_FILMS:
            html, status = await fetch(session, LIST_URL if page == 1 else f"{LIST_URL}page{page}/", attempt=1, backoffs=stats["backoffs"])
            if status == 429:
                stats["429"] += 1
            if status == 403:
                stats["403"] += 1
            if not html:
                logger.warning(f"[LIST] page={page} fetch=MISS; stop")
                break
            soup = BeautifulSoup(html, 'lxml')
            items = soup.select('div[data-test="ITEM"]')
            logger.info(f"[LIST] page={page} items_found={len(items)}")
            if not items:
                break
            for it in items:
                if len(films) >= MAX_FILMS:
                    break
                link = it.select_one('a[data-test="LINK ITEM-URL"]')
                if not link or not link.get('href'):
                    continue
                href = link['href']
                # clean fragment/params
                href = urlparse(href)._replace(query='', fragment='').geturl()
                # absolute
                detail = urljoin(BASE, href)
                s = slug_from_url(detail)
                # title from card if available
                name_link = it.select_one('[data-test="LINK ITEM-NAME ITEM-URL"]')
                title = name_link.get_text(" ", strip=True) if name_link else link.get('title') or s
                films.append(Film(title=title, url=detail, slug=s))
            page += 1
            pause = RATE_MIN + random.uniform(0.3, 0.9)
            stats["sleep_total"] += pause
            await asyncio.sleep(pause)

        logger.info(f"[LIST] total_candidates={len(films)} (limit={MAX_FILMS})")

        processed: List[Film] = []
        cb_failures = 0
        db = CacheDB(CACHE_DB)
        for i, f in enumerate(films, 1):
            logger.info(f"[DETAIL] {i}/{len(films)} slug={f.slug} url={f.url}")
            # Always compute region date URL for Perm
            date_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
            stats["region"].append((f.slug, date_url))

            # Fetch film detail (country/title/age/desc)
            html = None; status = -1
            for attempt in range(1, 3):
                html, status = await fetch(session, urljoin(BASE, f"/movie/{f.slug}/"), attempt, backoffs=stats["backoffs"])
                if status in (429, 403) or not html:
                    delay = RATE_MIN + attempt * 1.0 + random.uniform(0.5, 1.0)
                    stats["sleep_total"] += delay
                    await asyncio.sleep(delay)
                    continue
                break
            if not html:
                logger.warning(f"[PARSE] SKIP reason=DETAIL_FETCH_FAIL slug={f.slug}")
                continue
            soup = BeautifulSoup(html, 'lxml')

            # Extract meta
            country, c_via = parse_country_new(soup)
            age, a_via = parse_age_new(soup)
            desc, d_via = parse_desc_new(soup)
            title_override = parse_item_name(soup)
            if title_override:
                f.title = title_override

            # Fetch region date page
            date_html, d_status = await fetch(session, date_url, attempt=1, backoffs=stats["backoffs"])
            next_dt, n_via = (None, "miss")
            if date_html:
                date_soup = BeautifulSoup(date_html, 'lxml')
                next_dt, n_via = parse_first_day_new(date_soup)

            f.country = country
            f.age_limit = age
            f.description = desc
            f.next_date = next_dt
            stats["selectors"].append((f.slug, c_via, a_via, d_via, n_via))

            logger.info(
                f"[PARSE] title='{(f.title or '')[:40]}' country='{f.country}' age='{f.age_limit}' next='{f.next_date}' via={c_via}/{a_via}/{d_via}/{n_via}"
            )

            # Cache
            db.upsert_film(f.slug, f.title, f.country, None, f.description, f.age_limit, f.url)
            db.upsert_session(f.slug, f.next_date)

            # Filters
            if not f.country:
                logger.warning(f"[FILTER] EXCLUDE reason=NO_COUNTRY slug={f.slug}")
            elif not f.is_foreign:
                logger.info(f"[FILTER] EXCLUDE reason=RUSSIAN slug={f.slug} country='{f.country}'")
            elif not f.next_date:
                logger.info(f"[FILTER] EXCLUDE reason=NO_UPCOMING slug={f.slug}")
            else:
                processed.append(f)

            pause = RATE_MIN + random.uniform(0.3, 0.9)
            stats["sleep_total"] += pause
            await asyncio.sleep(pause)

        return processed, stats


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
        title = f.title
        if f.age_limit:
            title += f" ({f.age_limit})"
        ev.add('summary', title)
        desc_parts = []
        if f.country: desc_parts.append("Страна: " + f.country)
        if f.description: desc_parts.append("\nОписание: " + f.description)
        more_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
        desc_parts.append("\nПодробнее: " + more_url)
        ev.add('description', "\n".join(desc_parts))
        ev.add('url', more_url)
        ev.add('categories', ['ЗАРУБЕЖНЫЕ-ФИЛЬМЫ','КИНО','ПЕРМЬ'])
        cal.add_component(ev)

    docs_dir.mkdir(exist_ok=True)
    ics_path = docs_dir / "calendar.ics"
    payload = cal.to_ical()
    with open(ics_path, "wb") as fh:
        fh.write(payload)
    md5 = hashlib.md5(payload).hexdigest()
    logger.info(f"[ICS] events={len(films)} size={len(payload)} md5={md5} path={ics_path}")
    return ics_path


def write_index(docs_dir: Path, films_count: int, preview: List[str], stats: dict):
    preview_html = "".join(f"<li>{p}</li>" for p in preview)
    region_html = "".join(f"<li>{slug}: {url}</li>" for slug, url in stats.get('region', [])[:5])
    sel_html = "".join(
        f"<li>{slug}: country={c}, age={a}, desc={d}, date={n}</li>" for slug, c, a, d, n in stats.get('selectors', [])[:5]
    )
    html = (
        f"<!doctype html><html lang=\"ru\"><meta charset=\"utf-8\"><title>Календарь фильмов</title>\n"
        f"<body style=\"font-family:Arial,sans-serif;max-width:800px;margin:20px auto;\">\n"
        f"<h1>Календарь зарубежных фильмов (Пермь)</h1>\n"
        f"<p>Фильм(ов) в календаре: <strong>{films_count}</strong></p>\n"
        f"<p><a href=\"calendar.ics\">Скачать календарь (.ics)</a></p>\n"
        f"<h3>Пример событий</h3><ul>{preview_html}</ul>\n"
        f"<details><summary>REGION date URLs</summary><ul>{region_html}</ul></details>\n"
        f"<details><summary>Новые селекторы</summary><ul>{sel_html}</ul></details>\n"
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
        for f in films[:3]:
            more_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
            preview.append(f"{f.title} | {f.next_date} | {f.country} | {f.age_limit or ''} | {more_url}")

        ics_path = write_ics(films, docs)
        write_index(docs, len(films), preview, stats)

        diag.append("=== DIAG COPY START ===")
        diag.append(f"limit={MAX_FILMS} foreign_films={len(films)} 429={stats['429']} 403={stats['403']} cache_hits={stats['cache_hits']} cache_misses={stats['cache_misses']} sleep_total={stats['sleep_total']:.1f}")
        if stats['backoffs']:
            diag.append("429_backoffs=" + ",".join(f"{d:.1f}s" for d in stats['backoffs']))
        for slug, url in stats.get('region', [])[:5]:
            diag.append(f"REGION {slug} date_url={url}")
        for slug, c,a,d,n in stats.get('selectors', [])[:5]:
            diag.append(f"NEW-SELECTORS {slug} country={c} age={a} desc={d} date={n}")
        if preview:
            for p in preview:
                diag.append("PREVIEW " + p)
        diag.append(f"ICS path={ics_path} exists={ics_path.exists()} size={ics_path.stat().st_size if ics_path.exists() else 0}")
        diag.append("=== DIAG COPY END ===")
    except Exception as e:
        diag.append(f"FAIL AT=main exception={type(e).__name__} msg={e}")
    finally:
        write_diag(docs, diag)
        logger.info("\n" + "\n".join(diag))

if __name__ == "__main__":
    asyncio.run(main())
