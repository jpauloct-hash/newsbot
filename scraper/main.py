"""
NewsBot Financeiro — Scraper Principal
Executa via GitHub Actions a cada hora.

Fluxo:
  1. Lê RSS de cada fonte
  2. Filtra notícias já processadas (SQLite)
  3. Filtra por relevância financeira (pré-IA, economiza custo)
  4. Envia para Claude API → resumo estruturado
  5. Salva no SQLite
  6. Exporta news.json e feed.xml para o site estático
"""

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

# ──────────────────────────────────────────────
# CONFIGURAÇÃO
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "news.db"
JSON_PATH = DATA_DIR / "news.json"
RSS_PATH  = BASE_DIR / "feed.xml"

DATA_DIR.mkdir(exist_ok=True)

MAX_NEWS_PER_SOURCE = 5       # Máximo de notícias novas por fonte por execução
MAX_AGE_DAYS = 3              # Ignorar notícias mais antigas que X dias
MAX_ITEMS_IN_JSON = 300       # Máximo de itens no news.json (os mais recentes)
SITE_URL = os.environ.get("SITE_URL", "https://SEU-USUARIO.github.io/newsbot")


# ──────────────────────────────────────────────
# BANCO DE DADOS
# ──────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
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


def article_exists(conn: sqlite3.Connection, article_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM news WHERE id = ?", (article_id,)).fetchone()
    return row is not None


def save_article(conn: sqlite3.Connection, article: dict):
    conn.execute("""
        INSERT OR IGNORE INTO news
        (id, title, summary, category, relevance, keywords,
         source_id, source_name, url, published_at, collected_at)
        VALUES
        (:id, :title, :summary, :category, :relevance, :keywords,
         :source_id, :source_name, :url, :published_at, :collected_at)
    """, article)
    conn.commit()


# ──────────────────────────────────────────────
# UTILITÁRIOS
# ──────────────────────────────────────────────

def make_id(url: str, title: str) -> str:
    """Gera um ID único e estável para cada notícia."""
    return hashlib.sha256(f"{url}|{title}".encode()).hexdigest()[:16]


def is_financially_relevant(text: str) -> bool:
    """
    Filtro rápido pré-IA: verifica se o texto contém palavras financeiras.
    Evita gastar tokens da API em notícias irrelevantes.
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in RELEVANCE_KEYWORDS)


def parse_date(entry) -> str | None:
    """Tenta extrair a data de publicação de uma entrada RSS."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    return None


def is_too_old(date_str: str | None) -> bool:
    """Descarta notícias mais antigas que MAX_AGE_DAYS."""
    if not date_str:
        return False  # Se não souber a data, processa mesmo assim
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        return dt < cutoff
    except Exception:
        return False


# ──────────────────────────────────────────────
# SCRAPING
# ──────────────────────────────────────────────

def fetch_feed(source: dict) -> list[dict]:
    """
    Busca e parseia o feed RSS de uma fonte.
    Retorna lista de dicts com title, url, content, published_at.
    """
    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (financial news aggregator; contact via GitHub)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        response = requests.get(source["rss_url"], headers=headers, timeout=20)
        response.raise_for_status()

        feed = feedparser.parse(response.content)

        if feed.bozo and not feed.entries:
            logger.warning(f"[{source['id']}] Feed inválido ou vazio")
            return []

        articles = []
        for entry in feed.entries[:20]:  # Lê até 20 entradas por feed
            title   = getattr(entry, "title", "").strip()
            url     = getattr(entry, "link", "").strip()
            content = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            content = content.strip()
            pub_at  = parse_date(entry)

            if not title or not url:
                continue

            articles.append({
                "title": title,
                "url": url,
                "content": content,
                "published_at": pub_at,
            })

        logger.info(f"[{source['id']}] {len(articles)} entradas encontradas no feed")
        return articles

    except requests.RequestException as e:
        logger.error(f"[{source['id']}] Erro ao buscar feed: {e}")
        return []
    except Exception as e:
        logger.error(f"[{source['id']}] Erro inesperado: {e}")
        return []


# ──────────────────────────────────────────────
# EXPORTAÇÃO
# ──────────────────────────────────────────────

def export_json(conn: sqlite3.Connection):
    """Exporta as notícias mais recentes para data/news.json."""
    rows = conn.execute(f"""
        SELECT * FROM news
        ORDER BY collected_at DESC
        LIMIT {MAX_ITEMS_IN_JSON}
    """).fetchall()

    items = []
    for row in rows:
        items.append({
            "id":           row["id"],
            "title":        row["title"],
            "summary":      row["summary"],
            "category":     row["category"],
            "relevance":    row["relevance"],
            "keywords":     json.loads(row["keywords"] or "[]"),
            "source_id":    row["source_id"],
            "source_name":  row["source_name"],
            "url":          row["url"],
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

    logger.info(f"✅ Exportado: {JSON_PATH} ({len(items)} itens)")


def export_rss(conn: sqlite3.Connection):
    """Gera o feed RSS em feed.xml."""
    rows = conn.execute("""
        SELECT * FROM news
        ORDER BY collected_at DESC
        LIMIT 50
    """).fetchall()

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "NewsBot Financeiro"
    ET.SubElement(channel, "link").text = SITE_URL
    ET.SubElement(channel, "description").text = "Resumos imparciais de notícias financeiras e governamentais gerados por IA"
    ET.SubElement(channel, "language").text = "pt-BR"
    ET.SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    for row in rows:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = row["title"]
        ET.SubElement(item, "link").text = row["url"] or SITE_URL
        ET.SubElement(item, "description").text = row["summary"] or ""
        ET.SubElement(item, "category").text = row["category"] or ""
        ET.SubElement(item, "guid").text = row["id"]
        if row["published_at"]:
            try:
                dt = datetime.fromisoformat(row["published_at"].replace("Z", "+00:00"))
                ET.SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
            except Exception:
                pass

    tree = ET.ElementTree(rss)
    ET.indent(tree, space="  ")
    with open(RSS_PATH, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding="unicode", xml_declaration=False)

    logger.info(f"✅ Exportado: {RSS_PATH} ({len(rows)} itens)")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("🤖 NewsBot iniciando...")
    logger.info(f"   Fontes: {len(SOURCES)}")
    logger.info(f"   Max por fonte: {MAX_NEWS_PER_SOURCE}")
    logger.info("=" * 60)

    conn = init_db()

    total_new = 0
    total_skipped = 0
    total_errors = 0

    for source in SOURCES:
        logger.info(f"\n📡 [{source['id']}] {source['name']}")

        articles = fetch_feed(source)
        if not articles:
            continue

        processed_this_source = 0

        for article in articles:
            # Para se atingiu o limite desta fonte
            if processed_this_source >= MAX_NEWS_PER_SOURCE:
                break

            # Descarta notícias muito antigas
            if is_too_old(article["published_at"]):
                total_skipped += 1
                continue

            # Verifica duplicata
            article_id = make_id(article["url"], article["title"])
            if article_exists(conn, article_id):
                total_skipped += 1
                continue

            # Filtro pré-IA de relevância
            full_text = f"{article['title']} {article['content']}"
            if not is_financially_relevant(full_text):
                logger.debug(f"  ↷ Ignorado (sem relevância financeira): {article['title'][:60]}")
                total_skipped += 1
                continue

            # Chama Claude para resumir
            logger.info(f"  ✦ Resumindo: {article['title'][:70]}...")
            result = summarize(
                title=article["title"],
                content=article["content"],
                source_name=source["name"],
            )

            if not result:
                logger.warning(f"  ✗ Falha ao resumir")
                total_errors += 1
                continue

            # Descarta se a IA classificou como baixa relevância
            if result.get("relevancia") == "baixa":
                logger.debug(f"  ↷ Descartado pela IA (relevância baixa)")
                total_skipped += 1
                continue

            # Salva no banco
            save_article(conn, {
                "id":           article_id,
                "title":        article["title"],
                "summary":      result["resumo"],
                "category":     result["categoria"],
                "relevance":    result["relevancia"],
                "keywords":     json.dumps(result.get("keywords", []), ensure_ascii=False),
                "source_id":    source["id"],
                "source_name":  source["name"],
                "url":          article["url"],
                "published_at": article["published_at"],
                "collected_at": datetime.now(timezone.utc).isoformat(),
            })

            total_new += 1
            processed_this_source += 1
            logger.info(f"  ✓ Salvo [{result['categoria']}] [{result['relevancia']}]")

            # Pequena pausa para não estressar a API
            time.sleep(0.5)

    # Exporta os arquivos estáticos
    logger.info("\n" + "=" * 60)
    logger.info("📦 Exportando dados...")
    export_json(conn)
    export_rss(conn)
    conn.close()

    elapsed = round(time.time() - start_time, 1)

    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMO DA EXECUÇÃO")
    logger.info(f"   ✅ Novas notícias: {total_new}")
    logger.info(f"   ↷  Ignoradas:      {total_skipped}")
    logger.info(f"   ✗  Erros:          {total_errors}")
    logger.info(f"   ⏱  Tempo:          {elapsed}s")

    est = estimate_cost(total_new)
    logger.info(f"   💰 Custo estimado: ~${est['estimated_usd']} (~R${est['estimated_brl']})")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                main()