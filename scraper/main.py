"""
Coletor dedicado para os feeds RSS oficiais do Banco Central do Brasil.
Base:
- Página oficial de RSS do BC: https://www.bcb.gov.br/acessoinformacao/rss
"""

import logging
import re
from datetime import datetime, timezone, timedelta
from html import unescape

import feedparser
import requests

logger = logging.getLogger(__name__)

BCB_BASE = "https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds"
CURRENT_YEAR = datetime.now().year

BCB_FEEDS_SIMPLES = [
    ("comunicadoscopom", "Banco Central — Comunicados Copom", 90),
    ("atascopom", "Banco Central — Atas Copom", 120),
    ("indicadoresselecionados", "Banco Central — Indicadores Selecionados", 7),
    ("cambio", "Banco Central — Câmbio", 7),
    ("focus", "Banco Central — Relatório Focus", 10),
    ("notastecnicas", "Banco Central — Notas Técnicas", 45),
    ("ri", "Banco Central — Relatório de Inflação", 120),
    ("ref", "Banco Central — Relatório de Estabilidade Financeira", 180),
    ("boletimregional", "Banco Central — Boletim Regional", 120),
    ("resenhamercadoaberto", "Banco Central — Resenha Mercado Aberto", 14),
    ("blogdobc", "Banco Central — Blog do BC", 45),
    ("diarioeletronico", "Banco Central — Diário Eletrônico", 14),
]

BCB_FEEDS_COM_ANO = [
    ("noticias", "Banco Central — Notícias", 14),
    ("notasImprensa", "Banco Central — Notas à Imprensa", 30),
]


def _clean_text(text):
    text = unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_feed_date(entry):
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                continue

    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            raw = str(raw).strip()
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            except Exception:
                pass

    return None


def _is_too_old(date_str, max_age_days):
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        return dt < cutoff
    except Exception:
        return False


def _fetch_bcb_feed(url, source_id, source_name, max_age_days):
    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (+coletor-bcb)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }

        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()

        feed = feedparser.parse(response.content)

        if feed.bozo and not feed.entries:
            logger.warning("[%s] Feed inválido ou vazio: %s", source_id, url)
            return []

        articles = []

        for entry in feed.entries:
            title = _clean_text(getattr(entry, "title", "") or source_name)
            link = str(getattr(entry, "link", "") or "").strip()

            content = _clean_text(
                getattr(entry, "summary", "")
                or getattr(entry, "description", "")
                or title
            )

            published_at = _parse_feed_date(entry)

            if published_at and _is_too_old(published_at, max_age_days):
                continue

            if not title:
                continue

            articles.append({
                "title": title,
                "url": link,
                "content": content[:2000],
                "published_at": published_at,
                "source_id": source_id,
                "source_name": source_name,
            })

        logger.info("[%s] %d documentos coletados", source_id, len(articles))
        return articles

    except requests.RequestException as e:
        logger.error("[%s] Erro HTTP em %s: %s", source_id, url, e)
        return []
    except Exception as e:
        logger.exception("[%s] Erro ao processar feed RSS em %s: %s", source_id, url, e)
        return []


def _dedupe_articles(articles):
    unique = []
    seen = set()

    for article in articles:
        key = article.get("url") or (
            article.get("title"),
            article.get("published_at"),
            article.get("source_id"),
        )

        if key in seen:
            continue

        seen.add(key)
        unique.append(article)

    return unique


def fetch_all_bcb():
    all_articles = []

    for feed_name, source_name, max_age in BCB_FEEDS_SIMPLES:
        url = f"{BCB_BASE}/{feed_name}"
        source_id = f"bcb_{feed_name.lower()}"
        all_articles.extend(_fetch_bcb_feed(url, source_id, source_name, max_age))

    for year in (CURRENT_YEAR, CURRENT_YEAR - 1):
        for feed_name, source_name, max_age in BCB_FEEDS_COM_ANO:
            url = f"{BCB_BASE}/{feed_name}?ano={year}"
            source_id = f"bcb_{feed_name.lower()}"
            all_articles.extend(_fetch_bcb_feed(url, source_id, source_name, max_age))

    all_articles = _dedupe_articles(all_articles)
    logger.info("Total consolidado de documentos do BCB: %d", len(all_articles))
    return all_articles


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    items = fetch_all_bcb()
    for item in items[:10]:
        print(item["published_at"], "-", item["title"])