"""
Simplified scraper with structured diagnostics.
(Logs include DIAG COPY block at the end for quick sharing.)
"""

import asyncio
import logging
import os
import re
import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

LOG_LEVEL = os.getenv("MOVIE_SCRAPER_LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("movie_scraper.simple_scraper")

USER_AGENT = os.getenv("MOVIE_SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36")
ACCEPT_LANG = os.getenv("MOVIE_SCRAPER_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
PROXY_URL = os.getenv("MOVIE_SCRAPER_PROXY_URL")
RATE_MIN = float(os.getenv("MOVIE_SCRAPER_RATE_LIMIT", "1.0"))

BASE = "https://www.afisha.ru"
LIST_URL = f"{BASE}/prm/schedule_cinema/"

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}

class Film:
    def __init__(self, title: str, url: str):
        self.title = title
        self.url = url
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


async def fetch(session: aiohttp.ClientSession, url: str, attempt: int) -> Optional[str]:
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': ACCEPT_LANG,
        'Referer': LIST_URL,
        'DNT': '1',
        'Cache-Control': 'no-cache',
    }
    try:
        async with session.get(url, headers=headers, proxy=PROXY_URL) as resp:
            status = resp.status
            if status == 200:
                text = await resp.text()
                logger.info(f"[FETCH] try={attempt} status=200 url={url}")
                return text
            else:
                logger.warning(f"[FETCH] try={attempt} status={status} url={url}")
                return None
    except Exception as e:
        logger.warning(f"[FETCH] try={attempt} error={type(e).__name__} url={url} msg={e}")
        return None


def parse_country(soup: BeautifulSoup) -> Optional[str]:
    for dt in soup.select('dt'):
        if 'страна' in dt.get_text(strip=True).lower():
            dd = dt.find_next('dd')
            if dd:
                return dd.get_text(" ", strip=True)
    for sel in ['.film-info .country', '.movie-info .country', '.film-meta .country', '[data-country]']:
        el = soup.select_one(sel)
        if el:
            return el.get_text(" ", strip=True)
    el = soup.select_one('[itemprop="countryOfOrigin"]')
    if el:
        return el.get_text(" ", strip=True)
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"Страна\s*[:\-]?\s*([^\n,|]+(?:[,/;|][^\n,|]+)*)", txt, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def parse_age(soup: BeautifulSoup) -> Optional[str]:
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(\b\d{1,2}\+)\b", txt)
    return m.group(1) if m else None


def parse_rating(soup: BeautifulSoup) -> Optional[str]:
    txt = soup.get_text(" ", strip=True)
    for pat in [r"IMDb\s*[:\-]?\s*(\d+\.\d+)", r"Кинопоиск\s*[:\-]?\s*(\d+\.\d+)", r"рейтинг\s*[:\-]?\s*(\d+\.\d+)"]:
        m = re.search(pat, txt, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def parse_description(soup: BeautifulSoup) -> Optional[str]:
    for sel in ['.annotation', '.description', '.film-description', '.b-object-lead']:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(" ", strip=True)
            return (desc[:300] + '...') if len(desc) > 300 else desc
    return None


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


async def scrape() -> List[Film]:
    logger.info(f"[BOOT] py={os.sys.version.split()[0]} ua={USER_AGENT[:20]}… proxy={'on' if PROXY_URL else 'off'}")
    films: List[Film] = []
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        page = 1
        while page <= 10:
            url = LIST_URL if page == 1 else f"{LIST_URL}page{page}/"
            html = await fetch(session, url, attempt=1)
            if not html:
                logger.warning(f"[LIST] page={page} url={url} fetch=MISS; stop")
                break
            soup = BeautifulSoup(html, 'lxml')
            cards = soup.select('a[href*="/schedule_cinema_product/"]')
            logger.info(f"[LIST] page={page} films_found={len(cards)}")
            if not cards:
                break
            for a in cards:
                href = a.get('href')
                if not href:
                    continue
                detail = normalize_detail_url(urljoin(BASE, href))
                title = a.get_text(" ", strip=True) or ""
                films.append(Film(title=title, url=detail))
            page += 1
            await asyncio.sleep(RATE_MIN)

        films = films[:20]
        logger.info(f"[LIST] total_candidates={len(films)}")

        processed: List[Film] = []
        for i, f in enumerate(films, 1):
            logger.info(f"[DETAIL] {i}/{len(films)} url={f.url}")
            html = None
            for attempt in range(1, 4):
                await asyncio.sleep(RATE_MIN + (attempt * 0.3))
                html = await fetch(session, f.url, attempt)
                if html:
                    break
            if not html:
                logger.warning(f"[PARSE] SKIP reason=FETCH_FAIL url={f.url}")
                continue
            soup = BeautifulSoup(html, 'lxml')
            f.country = parse_country(soup)
            f.age_limit = parse_age(soup)
            f.rating = parse_rating(soup)
            f.description = parse_description(soup)
            f.next_date = parse_next_date(soup)
            logger.info(
                f"[PARSE] title='{(f.title or '')[:40]}' country='{f.country}' age='{f.age_limit}' rating='{f.rating}' next='{f.next_date}'"
            )
            if not f.country:
                logger.warning(f"[FILTER] EXCLUDE reason=NO_COUNTRY url={f.url}")
                continue
            if not f.is_foreign:
                logger.info(f"[FILTER] EXCLUDE reason=RUSSIAN url={f.url} country='{f.country}'")
                continue
            if not f.next_date:
                logger.info(f"[FILTER] EXCLUDE reason=NO_UPCOMING url={f.url}")
                continue
            processed.append(f)
        return processed


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
        slug = normalize_detail_url(f.url).rstrip('/').split('/')[-1]
        dt = f.next_date or date.today()
        ev.add('uid', f"{slug}-{dt.isoformat()}@perm-cinema")
        ev.add('dtstart', dt); ev['dtstart'].params['VALUE'] = 'DATE'
        ev.add('dtend', dt);   ev['dtend'].params['VALUE'] = 'DATE'
        ev.add('dtstamp', datetime.utcnow())
        title = f.title
        if f.age_limit:
            title += f" ({f.age_limit})"
        ev.add('summary', title)
        desc_parts = []
        if f.country: desc_parts.append("Страна: " + f.country)
        if f.rating:  desc_parts.append("Рейтинг: " + str(f.rating))
        if f.description: desc_parts.append("\nОписание: " + f.description)
        if f.url: desc_parts.append("\nПодробнее: " + f.url)
        ev.add('description', "\n".join(desc_parts))
        if f.url:
            ev.add('url', f.url)
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


def write_index(docs_dir: Path, films_count: int):
    # Use double braces to escape in f-string
    html = (
        f"<!doctype html><html lang=\"ru\"><meta charset=\"utf-8\"><title>Календарь фильмов</title>\n"
        f"<body style=\"font-family:Arial,sans-serif;max-width:800px;margin:20px auto;\">\n"
        f"<h1>Календарь зарубежных фильмов (Пермь)</h1>\n"
        f"<p>Фильм(ов) в календаре: <strong>{films_count}</strong></p>\n"
        f"<p><a href=\"calendar.ics\">Скачать календарь (.ics)</a></p>\n"
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
    docs = Path("docs")
    docs.mkdir(exist_ok=True)
    diag: List[str] = []
    try:
        diag.append(f"BOOT py={os.sys.version.split()[0]} ua={USER_AGENT[:30]}… proxy={'on' if PROXY_URL else 'off'}")
        films = await scrape()
        foreign = films
        diag.append(f"SUMMARY candidates_processed={len(foreign)}")

        ics_path = write_ics(foreign, docs)
        write_index(docs, len(foreign))

        diag.append("=== DIAG COPY START ===")
        diag.append(f"pages_scraped=~ films_discovered=~ foreign_films={len(foreign)}")
        for f in foreign[:5]:
            diag.append(f"FILM title='{f.title[:40]}' country='{f.country}' next='{f.next_date}' url='{f.url}'")
        diag.append(f"ICS path={ics_path} exists={ics_path.exists()} size={ics_path.stat().st_size if ics_path.exists() else 0}")
        diag.append("=== DIAG COPY END ===")
    except Exception as e:
        diag.append(f"FAIL AT=main exception={type(e).__name__} msg={e}")
    finally:
        write_diag(docs, diag)
        logger.info("\n" + "\n".join(diag))

if __name__ == "__main__":
    asyncio.run(main())
