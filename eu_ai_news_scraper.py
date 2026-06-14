"""
EU AI News Scraper
==================
Scrapes Google News RSS feeds for AI-related articles across all EU countries.

Install dependencies:
    pip install feedparser requests pandas schedule

Run once:
    python eu_ai_news_scraper.py

Run on a schedule (every 6 hours):
    python eu_ai_news_scraper.py --schedule 6

Outputs:
    - eu_ai_news.csv        (flat CSV for Excel/analysis)
    - eu_ai_news.json       (full structured data)
    - eu_ai_news.db         (SQLite for persistent deduplication)
"""

import feedparser
import sqlite3
import hashlib
import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

# ── Configuration ────────────────────────────────────────────────────────────

# AI-related keywords to filter articles (matched against title + description)
AI_KEYWORDS = [
    "artificial intelligence", "machine learning", "deep learning",
    "neural network", "generative ai", "large language model", "llm",
    "chatgpt", "gpt-4", "claude", "gemini", "mistral", "llama",
    "ai regulation", "eu ai act", "ai act", "ai policy",
    "robotics", "automation", "algorithm", "computer vision",
    "natural language processing", "nlp", "transformer model",
    # Translations of "artificial intelligence" — all 24 EU official languages
    "künstliche intelligenz",        # German (DE)
    "intelligence artificielle",     # French (FR)
    "intelligenza artificiale",      # Italian (IT)
    "inteligencia artificial",       # Spanish (ES)
    "sztuczna inteligencja",         # Polish (PL)
    "kunstmatige intelligentie",     # Dutch (NL)
    "inteligência artificial",       # Portuguese (PT)
    "umělá inteligence",             # Czech (CS)
    "kunstig intelligens",           # Danish (DA)
    "tehisintellekt",                # Estonian (ET)
    "tekoäly",                       # Finnish (FI)
    "τεχνητή νοημοσύνη",             # Greek (EL)
    "mesterséges intelligencia",     # Hungarian (HU)
    "inteligencia shaorga",          # Irish (GA)
    "mākslīgais intelekts",          # Latvian (LV)
    "dirbtinis intelektas",          # Lithuanian (LT)
    "intelliġenza artifiċjali",      # Maltese (MT)
    "umelá inteligencia",            # Slovak (SK)
    "umetna inteligenca",            # Slovenian (SL)
    "artificiell intelligens",       # Swedish (SV)
    "inteligență artificială",       # Romanian (RO)
    "изкуствен интелект",            # Bulgarian (BG)
    "umjetna inteligencija",         # Croatian (HR)
]

# All 27 EU member states with their Google News locale codes
EU_COUNTRIES = [
    {"name": "Germany",        "code": "DE", "lang": "de", "hl": "de", "gl": "DE", "ceid": "DE:de"},
    {"name": "France",         "code": "FR", "lang": "fr", "hl": "fr", "gl": "FR", "ceid": "FR:fr"},
    {"name": "Italy",          "code": "IT", "lang": "it", "hl": "it", "gl": "IT", "ceid": "IT:it"},
    {"name": "Spain",          "code": "ES", "lang": "es", "hl": "es", "gl": "ES", "ceid": "ES:es"},
    {"name": "Poland",         "code": "PL", "lang": "pl", "hl": "pl", "gl": "PL", "ceid": "PL:pl"},
    {"name": "Netherlands",    "code": "NL", "lang": "nl", "hl": "nl", "gl": "NL", "ceid": "NL:nl"},
    {"name": "Belgium",        "code": "BE", "lang": "fr", "hl": "fr", "gl": "BE", "ceid": "BE:fr"},
    {"name": "Sweden",         "code": "SE", "lang": "sv", "hl": "sv", "gl": "SE", "ceid": "SE:sv"},
    {"name": "Austria",        "code": "AT", "lang": "de", "hl": "de", "gl": "AT", "ceid": "AT:de"},
    {"name": "Denmark",        "code": "DK", "lang": "da", "hl": "da", "gl": "DK", "ceid": "DK:da"},
    {"name": "Finland",        "code": "FI", "lang": "fi", "hl": "fi", "gl": "FI", "ceid": "FI:fi"},
    {"name": "Portugal",       "code": "PT", "lang": "pt", "hl": "pt", "gl": "PT", "ceid": "PT:pt"},
    {"name": "Czech Republic", "code": "CZ", "lang": "cs", "hl": "cs", "gl": "CZ", "ceid": "CZ:cs"},
    {"name": "Romania",        "code": "RO", "lang": "ro", "hl": "ro", "gl": "RO", "ceid": "RO:ro"},
    {"name": "Hungary",        "code": "HU", "lang": "hu", "hl": "hu", "gl": "HU", "ceid": "HU:hu"},
    {"name": "Greece",         "code": "GR", "lang": "el", "hl": "el", "gl": "GR", "ceid": "GR:el"},
    {"name": "Slovakia",       "code": "SK", "lang": "sk", "hl": "sk", "gl": "SK", "ceid": "SK:sk"},
    {"name": "Ireland",        "code": "IE", "lang": "en", "hl": "en", "gl": "IE", "ceid": "IE:en"},
    {"name": "Croatia",        "code": "HR", "lang": "hr", "hl": "hr", "gl": "HR", "ceid": "HR:hr"},
    {"name": "Bulgaria",       "code": "BG", "lang": "bg", "hl": "bg", "gl": "BG", "ceid": "BG:bg"},
    {"name": "Lithuania",      "code": "LT", "lang": "lt", "hl": "lt", "gl": "LT", "ceid": "LT:lt"},
    {"name": "Slovenia",       "code": "SI", "lang": "sl", "hl": "sl", "gl": "SI", "ceid": "SI:sl"},
    {"name": "Latvia",         "code": "LV", "lang": "lv", "hl": "lv", "gl": "LV", "ceid": "LV:lv"},
    {"name": "Estonia",        "code": "EE", "lang": "et", "hl": "et", "gl": "EE", "ceid": "EE:et"},
    {"name": "Cyprus",         "code": "CY", "lang": "el", "hl": "el", "gl": "CY", "ceid": "CY:el"},
    {"name": "Luxembourg",     "code": "LU", "lang": "fr", "hl": "fr", "gl": "LU", "ceid": "LU:fr"},
    {"name": "Malta",          "code": "MT", "lang": "en", "hl": "en", "gl": "MT", "ceid": "MT:en"},
]

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("eu_ai_scraper")

# ── Database ─────────────────────────────────────────────────────────────────

DB_PATH = "eu_ai_news.db"

def init_db():
    """Create the SQLite database and articles table if they don't exist."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id          TEXT PRIMARY KEY,   -- SHA-256 of URL
            country     TEXT,
            country_code TEXT,
            title       TEXT,
            source      TEXT,
            url         TEXT,
            published   TEXT,
            summary     TEXT,
            scraped_at  TEXT
        )
    """)
    con.commit()
    return con

def article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def save_articles(con, articles: list[dict]):
    """Insert new articles, silently skip duplicates."""
    inserted = 0
    for a in articles:
        try:
            con.execute("""
                INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                a["id"], a["country"], a["country_code"],
                a["title"], a["source"], a["url"],
                a["published"], a["summary"], a["scraped_at"],
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # duplicate
    con.commit()
    return inserted

# ── Feed building ─────────────────────────────────────────────────────────────

def build_feed_url(country: dict, query: str = "artificial intelligence") -> str:
    """
    Build a Google News RSS URL for a specific country and query.
    Example: https://news.google.com/rss/search?q=artificial+intelligence&hl=de&gl=DE&ceid=DE:de
    """
    q = quote(query)
    return (
        f"https://news.google.com/rss/search"
        f"?q={q}"
        f"&hl={country['hl']}"
        f"&gl={country['gl']}"
        f"&ceid={country['ceid']}"
    )

# ── Keyword filtering ─────────────────────────────────────────────────────────

def matches_ai_keywords(title: str, summary: str) -> bool:
    """Return True if the article mentions at least one AI keyword."""
    text = (title + " " + summary).lower()
    return any(kw in text for kw in AI_KEYWORDS)

# ── Scraper ──────────────────────────────────────────────────────────────────

def scrape_country(country: dict, queries: list[str], delay: float = 1.0) -> list[dict]:
    """
    Scrape Google News RSS for a single country across all given queries.
    Returns a list of article dicts that match the AI keyword filter.
    """
    articles = []
    seen_urls = set()

    for query in queries:
        url = build_feed_url(country, query)
        try:
            feed = feedparser.parse(url)
            if feed.bozo and not feed.entries:
                log.warning(f"[{country['code']}] Feed parse issue for '{query}'")
                continue

            for entry in feed.entries:
                link = getattr(entry, "link", "")
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)

                title   = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")
                source  = getattr(entry, "source", {})
                source_name = source.get("title", "") if isinstance(source, dict) else str(source)
                published = getattr(entry, "published", "")

                if not matches_ai_keywords(title, summary):
                    continue

                articles.append({
                    "id":           article_id(link),
                    "country":      country["name"],
                    "country_code": country["code"],
                    "title":        title,
                    "source":       source_name,
                    "url":          link,
                    "published":    published,
                    "summary":      summary[:500],  # truncate for storage
                    "scraped_at":   datetime.utcnow().isoformat(),
                })

            log.info(f"[{country['code']}] '{query}' → {len(feed.entries)} entries, {len(articles)} kept so far")
            time.sleep(delay)

        except Exception as e:
            log.error(f"[{country['code']}] Error on '{query}': {e}")

    return articles

# ── Export ────────────────────────────────────────────────────────────────────

def export_json(articles: list[dict], path: str = "eu_ai_news.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    log.info(f"JSON saved → {path}")

def export_csv(articles: list[dict], path: str = "eu_ai_news.csv"):
    if not articles:
        log.warning("No articles to export to CSV.")
        return
    if HAS_PANDAS:
        pd.DataFrame(articles).to_csv(path, index=False, encoding="utf-8-sig")
    else:
        import csv
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=articles[0].keys())
            writer.writeheader()
            writer.writerows(articles)
    log.info(f"CSV saved  → {path}")

def export_from_db(con, json_path="eu_ai_news.json", csv_path="eu_ai_news.csv"):
    """Re-export everything in the database (useful for full snapshots)."""
    rows = con.execute("SELECT * FROM articles ORDER BY scraped_at DESC").fetchall()
    cols = [d[0] for d in con.execute("SELECT * FROM articles LIMIT 0").description]
    articles = [dict(zip(cols, r)) for r in rows]
    export_json(articles, json_path)
    export_csv(articles, csv_path)
    return articles

# ── Main scrape run ───────────────────────────────────────────────────────────

# Queries to run per country. Add or remove as needed.
QUERIES = [
    "artificial intelligence",
    "AI regulation",
    "machine learning",
    "EU AI Act",
    "generative AI",
]

def run_scrape(countries=None, max_workers=5, delay=1.0):
    """
    Run one full scrape cycle across all (or selected) EU countries.

    Args:
        countries:   List of country dicts to scrape (defaults to all EU_COUNTRIES).
        max_workers: Number of parallel scrapers.
        delay:       Seconds to wait between queries per country (be polite!).
    """
    if countries is None:
        countries = EU_COUNTRIES

    log.info(f"Starting scrape: {len(countries)} countries, {len(QUERIES)} queries each")
    con = init_db()
    all_new = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(scrape_country, c, QUERIES, delay): c
            for c in countries
        }
        for future in as_completed(futures):
            country = futures[future]
            try:
                articles = future.result()
                new = save_articles(con, articles)
                all_new.extend(articles[:new] if new else [])
                log.info(f"[{country['code']}] +{new} new articles saved")
            except Exception as e:
                log.error(f"[{country['code']}] Scrape failed: {e}")

    log.info(f"Scrape complete. {len(all_new)} new articles this run.")
    export_from_db(con)
    con.close()
    return all_new

# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EU AI News Scraper")
    parser.add_argument(
        "--schedule", type=int, metavar="HOURS",
        help="Run automatically every N hours (omit to run once and exit)"
    )
    parser.add_argument(
        "--countries", nargs="+", metavar="CODE",
        help="Limit to specific country codes, e.g. --countries DE FR PL"
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Number of parallel country scrapers (default: 5)"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds between requests per country (default: 1.0)"
    )
    args = parser.parse_args()

    # Filter countries if requested
    countries = EU_COUNTRIES
    if args.countries:
        codes = {c.upper() for c in args.countries}
        countries = [c for c in EU_COUNTRIES if c["code"] in codes]
        if not countries:
            log.error(f"No matching countries found for: {args.countries}")
            return

    if args.schedule:
        if not HAS_SCHEDULE:
            log.error("Install 'schedule' to use --schedule: pip install schedule")
            return
        import schedule as sched
        log.info(f"Scheduling scrape every {args.schedule} hour(s). Press Ctrl+C to stop.")
        run_scrape(countries, args.workers, args.delay)  # run immediately
        sched.every(args.schedule).hours.do(run_scrape, countries, args.workers, args.delay)
        while True:
            sched.run_pending()
            time.sleep(60)
    else:
        run_scrape(countries, args.workers, args.delay)

if __name__ == "__main__":
    main()
