import os
import sys
import json
import hashlib
import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import feedparser
import requests
from sources import SOURCES, RELEVANCE_KEYWORDS
from summarizer import summarize, estimate_cost

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "news.db"
JSON_PATH = DATA_DIR / "news.json"
RSS_PATH = BASE_DIR / "feed.xml"

DATA_DIR.mkdir(exist_ok=True)

MAX_NEWS_PER_SOURCE = 5
MAX_AGE_DAYS = 3
MAX_ITEMS_IN_JSON = 300
SITE_URL = os.environ.get("SITE_URL", "https://jpauloct-hash.github.io/newsbot")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id            TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            summary       TEXT,
            category      TEXT,
            relevance     TEXT,
            keywords      TEXT,
            source_id     TEXT NOT NULL,
            source_name   TEXT NOT NULL,
            url           TEXT,
            published_at  TEXT,
            collected_at  TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_collected ON news(collected_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON news(source_id)")
    conn.commit()
    return conn


def article_exists(conn, article_id):
    row = conn.execute("SELECT 1 FROM news WHERE id = ?", (article_id,)).fetchone()
    return row is not None


def save_article(conn, article):
    conn.execute("""
        INSERT OR IGNORE INTO news
        (id, title, summary, category, relevance, keywords,
         source_id, source_name, url, published_at, collected_at)
        VALUES
        (:id, :title, :summary, :category, :relevance, :keywords,
         :source_id, :source_name, :url, :published_at, :collected_at)
    """, article)
    conn.commit()


def make_id(url, title):
    return hashlib.sha256(f"{url}|{title}".encode()).hexdigest()[:16]


def is_financially_relevant(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANCE_KEYWORDS)


def parse_date(entry):
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    return None


def is_too_old(date_str):
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        return dt < cutoff
    except Exception:
        return False


def fetch_feed(source):
    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (financial news aggregator)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        response = requests.get(source["rss_url"], headers=headers, timeout=20)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        if feed.bozo and not feed.entries:
            logger.warning(f"[{source['id']}] Feed invalido ou vazio")
            return []
        articles = []
        for entry in feed.entries[:20]:
            title = getattr(entry, "title", "").strip()
            url = getattr(entry, "link", "").strip()
            content = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            content = content.strip()
            pub_at = parse_date(entry)
            if not title or not url:
                continue
            articles.append({
                "title": title,
                "url": url,
                "content": content,
                "published_at": pub_at,
            })
        logger.info(f"[{source['id']}] {len(articles)} entradas encontradas")
        return articles
    except Exception as e:
        logger.error(f"[{source['id']}] Erro: {e}")
        return []


def export_json(conn):
    rows = conn.execute(
        f"SELECT * FROM news ORDER BY collected_at DESC LIMIT {MAX_ITEMS_IN_JSON}"
    ).fetchall()
    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "title": row["title"],
            "summary": row["summary"],
            "category": row["category"],
            "relevance": row["relevance"],
            "keywords": json.loads(row["keywords"] or "[]"),
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "url": row["url"],
            "published_at": row["published_at"],
            "collected_at": row["collected_at"],
        })
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(items),
        "items": items,
    }
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"Exportado: {JSON_PATH} ({len(items)} itens)")


def export_rss(conn):
    rows = conn.execute(
        "SELECT * FROM news ORDER BY collected_at DESC LIMIT 50"
    ).fetchall()
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "NewsBot Financeiro"
    ET.SubElement(channel, "link").text = SITE_URL
    ET.SubElement(channel, "description").text = "Resumos de noticias financeiras gerados por IA"
    ET.SubElement(channel, "language").text = "pt-BR"
    ET.SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )
    for row in rows:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = row["title"]
        ET.SubElement(item, "link").text = row["url"] or SITE_URL
        ET.SubElement(item, "description").text = row["summary"] or ""
        ET.SubElement(item, "category").text = row["category"] or ""
        ET.SubElement(item, "guid").text = row["id"]
    tree = ET.ElementTree(rss)
    ET.indent(tree, space="  ")
    with open(RSS_PATH, "w", encoding="utf-8") as f:
    tree.write(f, encoding="unicode", xml_declaration=True)
    logger.info(f"Exportado: {RSS_PATH} ({len(rows)} itens)")


def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("NewsBot iniciando...")
    logger.info(f"   Fontes: {len(SOURCES)}")
    logger.info("=" * 60)

    conn = init_db()

    total_new = 0
    total_skipped = 0
    total_errors = 0

    for source in SOURCES:
        logger.info(f"\n[{source['id']}] {source['name']}")
        articles = fetch_feed(source)
        if not articles:
            continue

        processed_this_source = 0

        for article in articles:
            if processed_this_source >= MAX_NEWS_PER_SOURCE:
                break
            if is_too_old(article["published_at"]):
                total_skipped += 1
                continue
            article_id = make_id(article["url"], article["title"])
            if article_exists(conn, article_id):
                total_skipped += 1
                continue
            full_text = f"{article['title']} {article['content']}"
            if not is_financially_relevant(full_text):
                total_skipped += 1
                continue
            logger.info(f"  Resumindo: {article['title'][:70]}...")
            result = summarize(
                title=article["title"],
                content=article["content"],
                source_name=source["name"],
            )
            if not result:
                total_errors += 1
                continue
            if result.get("relevancia") == "baixa":
                total_skipped += 1
                continue
            save_article(conn, {
                "id": article_id,
                "title": article["title"],
                "summary": result["resumo"],
                "category": result["categoria"],
                "relevance": result["relevancia"],
                "keywords": json.dumps(result.get("keywords", []), ensure_ascii=False),
                "source_id": source["id"],
                "source_name": source["name"],
                "url": article["url"],
                "published_at": article["published_at"],
                "collected_at": datetime.now(timezone.utc).isoformat(),
            })
            total_new += 1
            processed_this_source += 1
            logger.info(f"  Salvo [{result['categoria']}] [{result['relevancia']}]")
            time.sleep(0.5)

    logger.info("\n" + "=" * 60)
    logger.info("Exportando dados...")
    export_json(conn)
    export_rss(conn)
    conn.close()

    elapsed = round(time.time() - start_time, 1)
    logger.info("=" * 60)
    logger.info("RESUMO DA EXECUCAO")
    logger.info(f"   Novas noticias: {total_new}")
    logger.info(f"   Ignoradas:      {total_skipped}")
    logger.info(f"   Erros:          {total_errors}")
    logger.info(f"   Tempo:          {elapsed}s")
    est = estimate_cost(total_new)
    logger.info(f"   Custo estimado: ~${est['estimated_usd']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()