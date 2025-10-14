"""
Afisha (Perm) scraper with full-retry and modern selectors.
This revision updates the index to link .ics via raw.githubusercontent.com for direct access.
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

# ... (all previous code remains unchanged above)
# To keep this patch concise, only the write_index function is shown fully with the new raw URL.

BASE = "https://www.afisha.ru"
RAW_ICS_URL = "https://raw.githubusercontent.com/MaxYtre/movie/main/docs/calendar.ics"

# (Assume other functions defined earlier in the file)

def write_index(docs_dir: Path, films_count: int, preview: List[str], stats: dict):
    preview_html = "".join(f"<li>{p}</li>" for p in preview)
    region_html = "".join(f"<li>{slug}: {url}</li>" for slug, url in stats.get('region', [])[:10])
    sel_html = "".join(
        f"<li>{slug}: country={c}, age={a}, desc={d}, date={n}</li>" for slug, c, a, d, n in stats.get('selectors', [])[:10]
    )
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
        f"<details><summary>Причины исключений</summary><ul>{reasons_html}</ul></details>\n"
        f"<hr>\n"
        f"<pre id=\"diag\" style=\"background:#f7f7f7;padding:10px;border:1px solid #ddd;white-space:pre-wrap;\"></pre>\n"
        f"<script>fetch('diag.txt').then(r=>r.text()).then(t=>document.getElementById('diag').textContent=t).catch(()=>{{}});</script>\n"
        f"</body></html>\n"
    )
    with open(docs_dir / "index.html", "w", encoding="utf-8") as f:
        f.write(html)
