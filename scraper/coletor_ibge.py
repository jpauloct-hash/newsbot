"""
Coletor dedicado para a API oficial do IBGE.

Docs:
- Notícias: https://servicodados.ibge.gov.br/api/docs/noticias?versao=3
- Calendário: https://servicodados.ibge.gov.br/api/docs/calendario?versao=3

Objetivo:
- Buscar notícias da Agência IBGE Notícias via API JSON
- Normalizar saída no mesmo formato usado pelo NewsBot
- Filtrar apenas conteúdos macroeconômicos/financeiros relevantes
- Remover conteúdo institucional/administrativo
- Opcionalmente buscar calendário de divulgações do IBGE
"""

import logging
import re
import hashlib
import unicodedata
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


# Termos que indicam conteúdo macroeconômico/financeiro relevante
IBGE_ALLOW_TERMS = [
    "ipca",
    "ipca-15",
    "inpc",
    "inflação",
    "inflacao",
    "preços",
    "precos",
    "pib",
    "contas nacionais",
    "desocupação",
    "desocupacao",
    "desemprego",
    "pnad",
    "pnad contínua",
    "pnad continua",
    "rendimento",
    "massa de rendimento",
    "varejo",
    "comércio",
    "comercio",
    "volume de vendas",
    "serviços",
    "servicos",
    "volume de serviços",
    "volume de servicos",
    "produção industrial",
    "producao industrial",
    "indústria",
    "industria",
    "pim",
    "pmc",
    "pms",
    "safra",
    "soja",
    "cereais",
    "oleaginosas",
    "agricultura",
    "agropecuária",
    "agropecuaria",
    "abate",
    "leite",
    "ovos",
    "produção",
    "producao",
]


# Termos que indicam conteúdo institucional, operacional ou administrativo
IBGE_BLOCK_TERMS = [
    "abre inscrições",
    "abre inscricoes",
    "inscrições",
    "inscricoes",
    "capacitação",
    "capacitacao",
    "curso",
    "oficina",
    "workshop",
    "seminário",
    "seminario",
    "webinário",
    "webinario",
    "encontro",
    "visita",
    "ministro",
    "sede do ibge",
    "presidente do ibge",
    "uso indevido",
    "logotipo",
    "concurso",
    "processo seletivo",
    "prova piloto",
    "recenseadores",
    "pesquisadores conhecem",
    "pesquisadores são acolhidos",
    "pesquisadores sao acolhidos",
    "equipes avançam",
    "equipes avancam",
    "mapa de riqueza de espécies",
    "mapa de riqueza de especies",
    "casa brasil ibge",
    "90 anos do ibge",
    "políticas culturais",
    "politicas culturais",
    "comunidade ribeirinha",
    "comunidades ribeirinhas",
    "agricultura familiar",
    "moradores de comunidades",
    "estudantes",
    "lançamento",
    "lancamento",
    "lança",
    "lanca",
    "agenda estratégica",
    "agenda estrategica",
]


def _clean_text(text):
    text = unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_text(text):
    """
    Normaliza texto para comparação, filtro e deduplicação.
    Remove acentos, pontuação e espaços duplicados.
    """
    text = str(text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
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


def _is_relevant_ibge_news(title, content="", url=""):
    """
    Decide se uma notícia do IBGE é relevante para o NewsBot financeiro.

    Regra:
    - Se tiver termo bloqueado institucional/administrativo, descarta.
    - Se tiver termo permitido macroeconômico/financeiro, mantém.
    - Caso contrário, descarta.
    """
    text = _normalize_text(f"{title} {content} {url}")

    block_terms = [_normalize_text(term) for term in IBGE_BLOCK_TERMS]
    allow_terms = [_normalize_text(term) for term in IBGE_ALLOW_TERMS]

    if any(term in text for term in block_terms):
        return False

    if any(term in text for term in allow_terms):
        return True

    return False


def _classify_ibge_category(title, content=""):
    """
    Classificação simples e editorial para notícias do IBGE.
    Esta categoria pode ser usada como dica no pipeline principal.
    """
    text = _normalize_text(f"{title} {content}")

    if any(x in text for x in ["ipca", "ipca 15", "inpc", "inflacao", "precos"]):
        return "Inflação"

    if any(x in text for x in ["desocupacao", "desemprego", "pnad", "rendimento", "massa de rendimento"]):
        return "Emprego"

    if any(x in text for x in ["pib", "contas nacionais"]):
        return "Atividade Econômica"

    if any(x in text for x in ["servicos", "volume de servicos", "pms"]):
        return "Atividade Econômica"

    if any(x in text for x in ["varejo", "comercio", "volume de vendas", "pmc"]):
        return "Atividade Econômica"

    if any(x in text for x in ["industria", "producao industrial", "pim"]):
        return "Atividade Econômica"

    if any(x in text for x in ["safra", "soja", "cereais", "oleaginosas", "agropecuaria", "abate", "leite", "ovos"]):
        return "Agro"

    return "Macro"


def _make_dedupe_key(title, url="", published_at=""):
    """
    Cria chave de deduplicação mais forte.
    Prioriza URL limpa. Se não houver URL, usa título normalizado + data.
    """
    clean_url = str(url or "").split("?")[0].strip().lower()

    if clean_url:
        return hashlib.md5(clean_url.encode("utf-8")).hexdigest()

    normalized_title = _normalize_text(title)
    raw_key = f"{normalized_title}|{published_at or ''}"
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()


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

            # Filtro editorial forte: descarta ruído institucional do IBGE
            if not _is_relevant_ibge_news(title, content, url):
                logger.debug("[ibge_noticias] Descartada por baixa relevância: %s", title)
                continue

            hay = _normalize_text(f"{title} {content}")
            matched_terms = [
                term for term in IBGE_ALLOW_TERMS
                if _normalize_text(term) in hay
            ][:6]

            category_hint = _classify_ibge_category(title, content)

            articles.append({
                "title": title,
                "url": url,
                "content": content[:2000],
                "published_at": published_at,
                "source_id": IBGE_SOURCE_ID,
                "source_name": IBGE_SOURCE_NAME,
                "keywords_hint": matched_terms,
                "category_hint": category_hint,
            })

        logger.info("[ibge_noticias] %d notícias relevantes coletadas", len(articles))
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
                _safe_get(
                    rec,
                    "titulo",
                    "nome",
                    "descricao",
                    "pesquisa",
                    default="Divulgação IBGE",
                )
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
    - aplica filtro macroeconômico/financeiro
    - remove ruído institucional
    - deduplica os resultados
    - não mistura calendário, para não poluir o feed principal
    """
    all_items = []

    # Notícias gerais
    all_items.extend(fetch_ibge_news(qtd=30, page=1))

    # Buscas adicionais focadas em temas macro.
    # Isso ajuda quando a primeira página vier muito institucional.
    search_terms = [
        "pib",
        "ipca",
        "ipca-15",
        "pnad",
        "produção industrial",
        "varejo",
        "serviços",
        "desemprego",
        "safra",
    ]

    for term in search_terms:
        all_items.extend(fetch_ibge_news(qtd=10, page=1, busca=term))

    # Deduplicação consolidada
    seen = set()
    deduped = []

    for item in all_items:
        key = _make_dedupe_key(
            item.get("title", ""),
            item.get("url", ""),
            item.get("published_at", ""),
        )

        if key in seen:
            continue

        seen.add(key)
        deduped.append(item)

    # Ordena do mais recente para o mais antigo, quando houver data
    deduped.sort(
        key=lambda x: x.get("published_at") or "",
        reverse=True,
    )

    logger.info("[ibge] Total consolidado: %d itens relevantes", len(deduped))
    return deduped


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    items = fetch_all_ibge()

    for item in items[:20]:
        print(
            item.get("published_at"),
            "-",
            item.get("category_hint"),
            "-",
            item.get("title"),
    )
