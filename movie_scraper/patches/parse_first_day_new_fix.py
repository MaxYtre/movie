"""
Fix: pick the earliest available session date (>= today) on the Perm schedule page.
- Collect all enabled day buttons and parse dates
- Return min date instead of the first DOM match
"""

# ... imports and constants from main branch are assumed present above ...
from datetime import date
from bs4 import BeautifulSoup
import re

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}

def parse_first_day_new(soup: BeautifulSoup):
    """Return the earliest enabled calendar day >= today.
    Falls back to regex scan if needed.
    """
    today = date.today()
    candidates = []

    # 1) Collect all enabled day buttons
    for a in soup.select('a[data-test="DAY"]:not([disabled])'):
        label = (a.get('aria-label') or a.get_text(" ", strip=True) or '').strip().lower()
        m = re.match(r"(\d{1,2})\s+([а-я]+)", label)
        if not m:
            continue
        d = int(m.group(1)); mon_name = m.group(2)
        mon = MONTHS_RU.get(mon_name)
        if not mon:
            continue
        try:
            cand = date(today.year, mon, d)
            if cand >= today:
                candidates.append(cand)
        except ValueError:
            continue

    if candidates:
        return min(candidates), "calendar-all"

    # 2) Fallback: scan full text for any future dates
    txt = soup.get_text(" ", strip=True)
    for m in re.finditer(r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)", txt, re.IGNORECASE):
        d = int(m.group(1)); mon = MONTHS_RU[m.group(2).lower()]
        try:
            cand = date(today.year, mon, d)
            if cand >= today:
                candidates.append(cand)
        except ValueError:
            pass

    return (min(candidates), "fallback-scan") if candidates else (None, "miss")
