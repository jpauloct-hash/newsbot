"""
Coletor dedicado para a API oficial do IBGE.

Docs:
- Notícias: https://servicodados.ibge.gov.br/api/docs/noticias?versao=3
- Calendário: https://servicodados.ibge.gov.br/api/docs/calendario?versao=3

Objetivo:
- Buscar notícias da Agência IBGE Notícias via API JSON
- Normalizar saída no mesmo formato usado pelo NewsBot
- Opcionalmente buscar calendário de divulgações do IBGE
"""

import logging
import re
from datetime import datetime, timezone, timedelta
from html import unescape
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

IBGE_BASE = "https://servicodados.ibge.gov.br/api/v3"
IBGE_NEWS_URL = f"{IBGE_BASE}/noticias/"
IBGE_CALENDAR_URL = f"{IBGE_BASE}/calendario/"

DEFAULT_TIMEOUT = 20

# Janelas de corte
IBGE_MAX_AGE_DAYS = 15
IBGE_CALENDAR_MAX_FUTURE_DAYS = 45

# Parâmetros padrão para notícias
IBGE_DEFAULT_QTD = 30
IBGE_DEFAULT_PAGE = 1
IBGE_DEFAULT_INTROSIZE = 1200

# Fonte padrão
IBGE_SOURCE_ID = "ibge_noticias"
IBGE_SOURCE_NAME = "IBGE — Agência de Notícias"

# Alguns produtos/pesquisas importantes podem ser filtrados depois no pipeline
IBGE_PRIORITY_TERMS = [
    "pib",
    "ipca",
    "ipca-15",
    "pnad",
    "pnad contínua",
    "pnad continua",
    "produção industrial",
    "producao industrial",
    "varejo",
    "serviços",
    "servicos",
    "desemprego",
    "renda",
    "inflação",
    "inflacao",
]


def _clean_text(text):
    text = unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _safe_get(d, *keys, default=None):
    for key in keys:
        if isinstance(d, dict) and key in d and d[key] not in (None, ""):
            return d[key]
    return default


def _parse_date(date_str):
    """
    Converte datas da API do IBGE para ISO 8601 UTC.
    """
    if not date_str:
        return None

    raw = str(date_str).strip()
    if not raw:
        return None

    # ISO nativo
    try:
        candidate = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    # Formatos alternativos
    known_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    )
    for fmt in known_formats:
        try:
            dt = datetime.strptime(raw[:19], fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    logger.debug("Não foi possível converter data do IBGE: %r", raw)
    return None


def _normalize_url(url):
    url = str(url or "").strip()
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urljoin("https://agenciadenoticias.ibge.gov.br/", url)


def _extract_news_records(data):
    """
    A API de notícias retorna paginação e coleção de itens.
    Aceitamos estruturas variadas para robustez.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return (
            data.get("items")
            or data.get("noticias")
            or data.get("dados")
            or data.get("results")
            or []
        )

    return []


def _extract_calendar_records(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return (
            data.get("items")
            or data.get("eventos")
            or data.get("dados")
            or data.get("results")
            or []
        )

    return []


def _is_too_old(iso_date, max_age_days):
    if not iso_date:
        return False
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        return dt < cutoff
    except Exception:
        return False


def _is_too_far_in_future(iso_date, max_future_days):
    if not iso_date:
        return False
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        limit = datetime.now(timezone.utc) + timedelta(days=max_future_days)
        return dt > limit
    except Exception:
        return False


def fetch_ibge_news(
    qtd=IBGE_DEFAULT_QTD,
    page=IBGE_DEFAULT_PAGE,
    busca=None,
    destaque=None,
    de=None,
    ate=None,
    introsize=IBGE_DEFAULT_INTROSIZE,
    idproduto=None,
    max_age_days=IBGE_MAX_AGE_DAYS,
):
    """
    Busca notícias oficiais do IBGE e normaliza para o formato do NewsBot.
    """
    params = {
        "qtd": qtd,
        "page": page,
        "introsize": introsize,
    }

    if busca:
        params["busca"] = busca
    if destaque is not None:
        params["destaque"] = destaque
    if de:
        params["de"] = de
    if ate:
        params["ate"] = ate
    if idproduto:
        params["idproduto"] = idproduto

    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (+coletor-ibge)",
            "Accept": "application/json",
        }
        response = requests.get(
            IBGE_NEWS_URL,
            params=params,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        records = _extract_news_records(data)
        if not records:
            logger.warning("[ibge_noticias] Nenhuma notícia encontrada")
            return []

        articles = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            title = _clean_text(
                _safe_get(rec, "titulo", "title", default="IBGE")
            )

            url = _normalize_url(
                _safe_get(rec, "link", "url", "href", default="")
            )

            summary = _clean_text(
                _safe_get(rec, "introducao", "intro", "resumo", "descricao", default="")
            )

            content = _clean_text(
                _safe_get(
                    rec,
                    "texto",
                    "conteudo",
                    "introducao",
                    "intro",
                    "resumo",
                    "descricao",
                    default=summary or title,
                )
            )

            published_at = _parse_date(
                _safe_get(
                    rec,
                    "data_publicacao",
                    "dataPublicacao",
                    "data",
                    "published_at",
                    "publicado_em",
                    default="",
                )
            )

            if published_at and _is_too_old(published_at, max_age_days):
                continue

            # Guarda palavras-chave simples a partir do texto
            hay = f"{title} {content}".lower()
            matched_terms = [term for term in IBGE_PRIORITY_TERMS if term in hay][:6]

            articles.append({
                "title": title,
                "url": url,
                "content": content[:2000],
                "published_at": published_at,
                "source_id": IBGE_SOURCE_ID,
                "source_name": IBGE_SOURCE_NAME,
                "keywords_hint": matched_terms,
            })

        logger.info("[ibge_noticias] %d notícias coletadas", len(articles))
        return articles

    except requests.RequestException as e:
        logger.error("[ibge_noticias] Erro HTTP: %s", e)
        return []
    except ValueError as e:
        logger.error("[ibge_noticias] Erro ao decodificar JSON: %s", e)
        return []
    except Exception as e:
        logger.exception("[ibge_noticias] Erro inesperado: %s", e)
        return []


def fetch_ibge_calendar(
    pesquisa=None,
    qtd=20,
    de=None,
    ate=None,
    max_future_days=IBGE_CALENDAR_MAX_FUTURE_DAYS,
):
    """
    Busca calendário de divulgações do IBGE.
    Retorna no mesmo formato-base do projeto, para você decidir
    se quer salvar em outra tabela ou exibir separadamente.
    """
    url = f"{IBGE_CALENDAR_URL}{pesquisa}" if pesquisa else IBGE_CALENDAR_URL

    params = {"qtd": qtd}
    if de:
        params["de"] = de
    if ate:
        params["ate"] = ate

    try:
        headers = {
            "User-Agent": "NewsBot/1.0 (+coletor-ibge)",
            "Accept": "application/json",
        }
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        records = _extract_calendar_records(data)
        if not records:
            logger.warning("[ibge_calendario] Nenhum evento encontrado")
            return []

        events = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            title = _clean_text(
                _safe_get(rec, "titulo", "nome", "descricao", "pesquisa", default="Divulgação IBGE")
            )

            description = _clean_text(
                _safe_get(rec, "descricao", "detalhes", "observacao", default=title)
            )

            date_iso = _parse_date(
                _safe_get(
                    rec,
                    "data",
                    "data_publicacao",
                    "dataPublicacao",
                    "data_divulgacao",
                    default="",
                )
            )

            if date_iso and _is_too_far_in_future(date_iso, max_future_days):
                continue

            pesquisa_nome = _clean_text(
                _safe_get(rec, "pesquisa", "produto", default="IBGE")
            )

            events.append({
                "title": title,
                "url": "",
                "content": description[:2000],
                "published_at": date_iso,
                "source_id": "ibge_calendario",
                "source_name": f"IBGE — Calendário ({pesquisa_nome})",
            })

        logger.info("[ibge_calendario] %d eventos coletados", len(events))
        return events

    except requests.RequestException as e:
        logger.error("[ibge_calendario] Erro HTTP: %s", e)
        return []
    except ValueError as e:
        logger.error("[ibge_calendario] Erro ao decodificar JSON: %s", e)
        return []
    except Exception as e:
        logger.exception("[ibge_calendario] Erro inesperado: %s", e)
        return []


def fetch_all_ibge():
    """
    Coletor principal do IBGE para o pipeline atual.

    Por padrão:
    - busca notícias recentes da Agência IBGE Notícias
    - não mistura calendário, para não poluir o feed principal com agenda

    Se quiser, você pode depois acrescentar:
    all_items.extend(fetch_ibge_calendar(...))
    """
    all_items = []

    # Notícias gerais
    all_items.extend(fetch_ibge_news(qtd=30, page=1))

    # Busca adicional focada em temas macro mais prováveis
    # Ajuda quando a primeira página vier muito institucional.
    for term in ("pib", "ipca", "pnad", "produção industrial", "desemprego"):
        all_items.extend(fetch_ibge_news(qtd=10, page=1, busca=term))

    # Deduplicação
    seen = set()
    deduped = []

    for item in all_items:
        key = item.get("url") or (item.get("title"), item.get("published_at"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    logger.info("[ibge] Total consolidado: %d itens", len(deduped))
    return deduped


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    items = fetch_all_ibge()
    for item in items[:10]:
        print(item["published_at"], "-", item["title"])