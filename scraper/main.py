"""
Coletor dedicado para as APIs oficiais do Banco Central do Brasil.
URLs descobertas na pagina de RSS do proprio BCB.
Base: https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds/
"""

import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

BCB_BASE = "https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds"
CURRENT_YEAR = datetime.now().year

# Feeds prioritarios — sem parametro de ano (retornam dados recentes)
BCB_FEEDS_SIMPLES = [
    ("comunicadoscopom",     "Banco Central — Comunicados Copom",    90),
    ("atascopom",            "Banco Central — Atas Copom",           90),
    ("indicadoresselecionados", "Banco Central — Indicadores",        7),
    ("cambio",               "Banco Central — Cambio",                3),
    ("focus",                "Banco Central — Relatorio Focus",       7),
    ("notastecnicas",        "Banco Central — Notas Tecnicas",        30),
    ("ri",                   "Banco Central — Relatorio Inflacao",    90),
    ("ref",                  "Banco Central — Estabilidade Financeira", 180),
    ("boletimregional",      "Banco Central — Boletim Regional",      90),
    ("resenhamercadoaberto", "Banco Central — Resenha Mercado Aberto", 7),
    ("blogdobc",             "Banco Central — Blog do BC",            30),
    ("diarioeletronico",     "Banco Central — Diario Eletronico",     7),
]

# Feeds com parametro de ano
BCB_FEEDS_COM_ANO = [
    ("noticias",      "Banco Central — Noticias",     7),
    ("notasImprensa", "Banco Central — Notas Imprensa", 7),
]


def _parse_bcb_date(date_str):
    """Converte data do BCB para ISO format."""
    if not date_str:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ):
        try:
            dt = datetime.strptime(date_str[:19], fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def _fetch_bcb_feed(url, source_id, source_name, max_age_days):
    """Busca um feed JSON do BCB e normaliza os artigos."""
    try:
        headers = {"User-Agent": "NewsBot/1.0 (financial news aggregator)"}
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        # Estrutura pode ser lista ou {"conteudo": [...]} ou {"value": [...]}
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = (
                data.get("conteudo") or
                data.get("value") or
                data.get("items") or
                data.get("data") or
                []
            )
        else:
            records = []

        if not records:
            logger.warning(f"[{source_id}] Nenhum registro")
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        articles = []

        for rec in records:
            # Titulo
            title = (
                rec.get("titulo") or rec.get("Titulo") or
                rec.get("name") or rec.get("descricao") or
                source_name
            ).strip()

            # URL do documento
            link = (
                rec.get("url") or rec.get("Url") or
                rec.get("link") or rec.get("Link") or ""
            )
            if link and not link.startswith("http"):
                link = f"https://www.bcb.gov.br{link}"

            # Data
            date_str = (
                rec.get("dataPublicacao") or rec.get("DataPublicacao") or
                rec.get("data") or rec.get("Data") or
                rec.get("dataHora") or ""
            )
            published_at = _parse_bcb_date(date_str)

            # Descarta se muito antigo
            if published_at:
                try:
                    dt = datetime.fromisoformat(published_at)
                    if dt < cutoff:
                        continue
                except Exception:
                    pass

            # Conteudo
            import re
            content = (
                rec.get("conteudo") or rec.get("resumo") or
                rec.get("introducao") or rec.get("descricao") or title
            )
            content = re.sub(r"<[^>]+>", " ", str(content)).strip()

            articles.append({
                "title": title,
                "url": link,
                "content": content[:2000],
                "published_at": published_at,
                "source_id": source_id,
                "source_name": source_name,
            })

        logger.info(f"[{source_id}] {len(articles)} documentos")
        return articles

    except Exception as e:
        logger.error(f"[{source_id}] Erro: {e}")
        return []


def fetch_all_bcb():
    """
    Busca todos os feeds prioritarios do BCB.
    Retorna lista unica de artigos de todas as fontes.
    """
    all_articles = []

    # Feeds simples (sem ano)
    for feed_name, source_name, max_age in BCB_FEEDS_SIMPLES:
        url = f"{BCB_BASE}/{feed_name}"
        source_id = f"bcb_{feed_name.lower()}"
        articles = _fetch_bcb_feed(url, source_id, source_name, max_age)
        all_articles.extend(articles)

    # Feeds com ano
    for feed_name, source_name, max_age in BCB_FEEDS_COM_ANO:
        url = f"{BCB_BASE}/{feed_name}?ano={CURRENT_YEAR}"
        source_id = f"bcb_{feed_name.lower()}"
        articles = _fetch_bcb_feed(url, source_id, source_name, max_age)
        all_articles.extend(articles)

    return all_articles