from datetime import datetime

CURRENT_YEAR = datetime.now().year

# Fontes primárias oficiais — sem Google News
SOURCES = [
    # ──────────────────────────────────────────────
    # BANCO CENTRAL DO BRASIL — RSS oficial
    # ──────────────────────────────────────────────
    {
        "id": "bcb_noticias",
        "name": "Banco Central — Notícias",
        "category": "Banco Central",
        "rss_url": f"https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds/noticias?ano={CURRENT_YEAR}",
    },
    {
        "id": "bcb_notas",
        "name": "Banco Central — Notas",
        "category": "Banco Central",
        "rss_url": "https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds/notasImprensa",
    },

    # ❌ removido (estava quebrando)
    # {
    #     "id": "bcb_pronunciamentos",
    #     "name": "Banco Central — Pronunciamentos",
    #     "category": "Banco Central",
    #     "rss_url": "https://www.bcb.gov.br/api/feed/sitemap/pronunciamentos.xml",
    # },

    # ──────────────────────────────────────────────
    # IBGE — Agência de Notícias
    # ──────────────────────────────────────────────
    {
        "id": "ibge_noticias",
        "name": "IBGE — Notícias",
        "category": "Indicadores",
        "rss_url": "https://agenciadenoticias.ibge.gov.br/agencia-rss/2207-agencia-de-noticias/rss.xml",
    },
    {
        "id": "ibge_releases",
        "name": "IBGE — Releases",
        "category": "Indicadores",
        "rss_url": "https://agenciadenoticias.ibge.gov.br/agencia-rss/94-agencia-sala-de-imprensa/rss.xml",
    },
]

# Palavras-chave para filtro pre-IA
RELEVANCE_KEYWORDS = [
    # macro
    "selic", "ipca", "pib", "inflacao", "juro", "taxa",
    "copom", "politica monetaria", "cambio", "dolar", "reservas",

    # fiscal
    "fiscal", "divida", "deficit", "superavit", "arrecadacao",
    "orcamento", "tesouro", "resultado primario",

    # atividade
    "producao", "industria", "comercio", "servicos", "emprego",
    "desocupacao", "pnad", "pim", "pmc", "pms",

    # mercado
    "resultado", "lucro", "receita", "ebitda", "dividendo",
    "fato relevante", "ipo", "emissao", "rating",

    # regulatorio
    "resolucao", "instrucao", "regulacao", "normativa", "portaria",
    "deliberacao", "circular", "nota", "comunicado",

    # internacional
    "fed", "bce", "balanco", "exportacao", "importacao",
]