"""
Coletor dedicado para as APIs oficiais do Banco Central do Brasil.
URLs descobertas na página de RSS do próprio BCB.
Base: https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds/
"""

import logging
import re
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

BCB_BASE = "https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds"
CURRENT_YEAR = datetime.now().year

# Feeds prioritários — sem parâmetro de ano
BCB_FEEDS_SIMPLES = [
    ("comunicadoscopom", "Banco Central — Comunicados Copom", 90),
    ("atascopom", "Banco Central — Atas Copom", 90),
    ("indicadoresselecionados", "Banco Central — Indicadores", 7),
    ("cambio", "Banco Central — Câmbio", 3),
    ("focus", "Banco Central — Relatório Focus", 7),
    ("notastecnicas", "Banco Central — Notas Técnicas", 30),
    ("ri", "Banco Central — Relatório de Inflação", 90),
    ("ref", "Banco Central — Estabilidade Financeira", 180),
    ("boletimregional", "Banco Central — Boletim Regional", 90),
    ("resenhamercadoaberto", "Banco Central — Resenha Mercado Aberto", 7),
    ("blogdobc", "Banco Central — Blog do BC", 30),
    ("diarioeletronico", "Banco Central — Diário Eletrônico", 7),
]

# Feeds com parâmetro de ano
BCB_FEEDS_COM_ANO = [
    ("noticias", "Banco Central — Notícias", 7),
    ("notasImprensa", "Banco Central — Notas à Imprensa", 7),
]


def _parse_bcb_date(date_str):
    """
    Converte datas do BCB para ISO 8601 em UTC.
    Tenta primeiro formatos ISO nativos e depois formatos conhecidos.
    """
    if not date_str:
        return None

    raw = str(date_str).strip()
    if not raw:
        return None

    # 1) Tenta ISO completo, inclusive com timezone ou "Z"
    try:
        iso_candidate = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    # 2) Tenta formatos manuais conhecidos
    known_formats = (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    )

    for fmt in known_formats:
        try:
            # Mantém compatibilidade com strings maiores
            dt = datetime.strptime(raw[:19], fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    logger.debug("Não foi possível converter data do BCB: %r", raw)
    return None


def _extract_records(data):
    """
    Extrai registros da resposta JSON do BCB, aceitando estruturas variadas.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return (
            data.get("conteudo")
            or data.get("value")
            or data.get("items")
            or data.get("data")
            or []
        )

    return []


def _normalize_link(link):
    """
    Normaliza URLs relativas do BCB.
    """
    link = str(link or "").strip()
    if link and not link.startswith("http"):
        link = f"https://www.bcb.gov.br{link}"
    return link


def _clean_html(text):
    """
    Remove tags HTML simples e normaliza espaços.
    """
    text = str(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _safe_text(value, fallback=""):
    """
    Garante string limpa sem quebrar caso venha None ou tipo inesperado.
    """
    return str(value if value is not None else fallback).strip()


def _fetch_bcb_feed(url, source_id, source_name, max_age_days):
    """
    Busca um feed JSON do BCB e normaliza os artigos.
    """
    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (+coletor-financeiro-bcb)"
        }

        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()

        data = response.json()
        records = _extract_records(data)

        if not records:
            logger.warning("[%s] Nenhum registro encontrado em %s", source_id, url)
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        articles = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            title = _safe_text(
                rec.get("titulo")
                or rec.get("Titulo")
                or rec.get("name")
                or rec.get("descricao")
                or source_name
            )

            link = _normalize_link(
                rec.get("url")
                or rec.get("Url")
                or rec.get("link")
                or rec.get("Link")
                or ""
            )

            date_str = (
                rec.get("dataPublicacao")
                or rec.get("DataPublicacao")
                or rec.get("data")
                or rec.get("Data")
                or rec.get("dataHora")
                or rec.get("DataHora")
                or ""
            )
            published_at = _parse_bcb_date(date_str)

            if published_at:
                try:
                    dt = datetime.fromisoformat(published_at)
                    if dt < cutoff:
                        continue
                except Exception:
                    logger.exception("[%s] Falha ao validar data %r", source_id, published_at)

            content = _clean_html(
                rec.get("conteudo")
                or rec.get("resumo")
                or rec.get("introducao")
                or rec.get("descricao")
                or title
            )

            article = {
                "title": title,
                "url": link,
                "content": content[:2000],
                "published_at": published_at,
                "source_id": source_id,
                "source_name": source_name,
            }
            articles.append(article)

        logger.info("[%s] %d documentos coletados", source_id, len(articles))
        return articles

    except requests.RequestException as e:
        logger.error("[%s] Erro HTTP em %s: %s", source_id, url, e)
        return []

    except ValueError as e:
        logger.error("[%s] Erro ao decodificar JSON em %s: %s", source_id, url, e)
        return []

    except Exception as e:
        logger.exception("[%s] Erro inesperado em %s: %s", source_id, url, e)
        return []


def _dedupe_articles(articles):
    """
    Remove duplicatas com prioridade para URL.
    Se não houver URL, usa (title, published_at, source_id).
    """
    unique = []
    seen = set()

    for article in articles:
        key = (
            article.get("url")
            or (
                article.get("title"),
                article.get("published_at"),
                article.get("source_id"),
            )
        )

        if key in seen:
            continue

        seen.add(key)
        unique.append(article)

    return unique


def fetch_all_bcb():
    """
    Busca todos os feeds prioritários do BCB.
    Retorna lista única de artigos de todas as fontes.
    """
    all_articles = []

    # Feeds simples (sem ano)
    for feed_name, source_name, max_age in BCB_FEEDS_SIMPLES:
        url = f"{BCB_BASE}/{feed_name}"
        source_id = f"bcb_{feed_name.lower()}"
        articles = _fetch_bcb_feed(url, source_id, source_name, max_age)
        all_articles.extend(articles)

    # Feeds com ano
    # Consulta ano atual e anterior para evitar perda de itens na virada do ano
    for year in (CURRENT_YEAR, CURRENT_YEAR - 1):
        for feed_name, source_name, max_age in BCB_FEEDS_COM_ANO:
            url = f"{BCB_BASE}/{feed_name}?ano={year}"
            source_id = f"bcb_{feed_name.lower()}"
            articles = _fetch_bcb_feed(url, source_id, source_name, max_age)
            all_articles.extend(articles)

    all_articles = _dedupe_articles(all_articles)

    logger.info("Total consolidado de documentos do BCB: %d", len(all_articles))
    return all_articles