"""
Afisha (Perm) scraper with cache usage.
- Adds true cache hits/misses logic to skip HTTP when fresh.
"""

# (imports and constants are unchanged above)

# ... existing code ...

class CacheDB:
    # ... existing methods ...
    def get_film_row(self, slug: str):
        cur = self.conn.execute("SELECT slug,title,country,rating,description,age,url,updated_at FROM films WHERE slug=?", (slug,))
        return cur.fetchone()

# ... existing code ...

async def scrape() -> Tuple[List['Film'], dict]:
    stats = {"429": 0, "403": 0, "errors": 0, "cache_hits": 0, "cache_misses": 0, "sleep_total": 0.0, "backoffs": [], "selectors": [], "region": [], "reasons": []}
    logger.info(f"[BOOT] py={os.sys.version.split()[0]} ua={USER_AGENT_BASE[:20]}… proxy={'on' if PROXY_URL else 'off'}")
    films: List[Film] = []
    timeout = aiohttp.ClientTimeout(total=3600)
    connector = aiohttp.TCPConnector(limit=6)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # ... listing unchanged ...

        processed: List[Film] = []
        db = CacheDB(CACHE_DB)
        for i, f in enumerate(films, 1):
            logger.info(f"[DETAIL] {i}/{len(films)} slug={f.slug} url={f.url}")
            date_url = urljoin(BASE, f"/prm/schedule_cinema_product/{f.slug}/")
            stats["region"].append((f.slug, date_url))

            # --- New: Try cache first ---
            cached_ok = False
            if db.is_fresh(f.slug, CACHE_TTL_DAYS):
                row = db.get_film_row(f.slug)
                next_dt = db.get_session(f.slug)
                if row:
                    _, title, country, _, description, age, _, _ = row
                    f.title = title or f.title
                    f.country = country
                    f.description = description
                    f.age_limit = age
                    f.next_date = next_dt
                    stats["cache_hits"] += 1
                    logger.info(f"[CACHE] HIT slug={f.slug} next={f.next_date}")
                    # Selectors path labels for cache
                    stats["selectors"].append((f.slug, "cache", "cache", "cache", "cache" if next_dt else "cache-miss-date"))
                    cached_ok = True

            if cached_ok:
                keep = True
                if not f.country:
                    stats["reasons"].append((f.slug, "NO_COUNTRY")); keep = False
                elif not f.is_foreign:
                    stats["reasons"].append((f.slug, "NOT_FOREIGN")); keep = False
                elif not f.next_date:
                    stats["reasons"].append((f.slug, "NO_DATE")); keep = False
                if keep:
                    processed.append(f)
                # continue to next film (skip HTTP)
                pause = RATE_MIN + random.uniform(0.1, 0.3)
                stats["sleep_total"] += pause
                await asyncio.sleep(pause)
                continue

            stats["cache_misses"] += 1

            # --- Otherwise: fetch HTTP ---
            html = await robust_get(session, urljoin(BASE, f"/movie/{f.slug}/"), stats["backoffs"])
            if not html:
                stats["reasons"].append((f.slug, "DETAIL_FAIL"))
                continue
            soup = BeautifulSoup(html, 'lxml')
            country, c_via = parse_country_new(soup)
            age, a_via = parse_age_new(soup)
            desc, d_via = parse_desc_new(soup)
            title_override = parse_item_name(soup)
            if title_override:
                f.title = title_override

            date_html = await robust_get(session, date_url, stats["backoffs"])
            next_dt, n_via = (None, "miss")
            if date_html:
                date_soup = BeautifulSoup(date_html, 'lxml')
                next_dt, n_via = parse_first_day_new(date_soup)
            else:
                stats["reasons"].append((f.slug, "DATE_FAIL"))

            f.country = country
            f.age_limit = age
            f.description = desc
            f.next_date = next_dt
            stats["selectors"].append((f.slug, c_via, a_via, d_via, n_via))

            keep = True
            if not f.country:
                stats["reasons"].append((f.slug, "NO_COUNTRY")); keep = False
            elif not f.is_foreign:
                stats["reasons"].append((f.slug, "NOT_FOREIGN")); keep = False
            elif not f.next_date:
                stats["reasons"].append((f.slug, "NO_DATE")); keep = False

            db.upsert_film(f.slug, f.title, f.country, None, f.description, f.age_limit, f.url)
            db.upsert_session(f.slug, f.next_date)

            if keep:
                processed.append(f)

            pause = RATE_MIN + random.uniform(0.3, 0.9)
            stats["sleep_total"] += pause
            await asyncio.sleep(pause)

        return processed, stats
