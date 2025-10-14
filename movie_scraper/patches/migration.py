# Migration helper for development branch: ensure enrichment columns exist

def ensure_enrichment_columns(conn):
    cur = conn.execute("PRAGMA table_info(films)")
    cols = {row[1] for row in cur.fetchall()}
    add = []
    if 'imdb_rating' not in cols: add.append("ALTER TABLE films ADD COLUMN imdb_rating REAL")
    if 'kp_rating' not in cols: add.append("ALTER TABLE films ADD COLUMN kp_rating REAL")
    if 'trailer_url' not in cols: add.append("ALTER TABLE films ADD COLUMN trailer_url TEXT")
    if 'poster_url' not in cols: add.append("ALTER TABLE films ADD COLUMN poster_url TEXT")
    if 'year' not in cols: add.append("ALTER TABLE films ADD COLUMN year INTEGER")
    for ddl in add:
        conn.execute(ddl)
    if add:
        conn.commit()
