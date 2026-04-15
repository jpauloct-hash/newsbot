"""
Fontes oficiais de notícias e atos regulatórios para o NewsBot.

IMPORTANTE:
- Este arquivo foi desenhado para o main.py atual, que espera feeds RSS/XML
  no campo `rss_url` e processa tudo com feedparser.
- O Banco Central ficou de fora daqui porque já é coletado separadamente
  por `fetch_all_bcb()`.
- A API do IBGE é oficial e muito útil, mas é JSON, não RSS. Então ela
  deve entrar por um coletor dedicado, não por este SOURCES.
"""

SOURCES = [
    # ──────────────────────────────────────────────
    # CVM — feeds RSS oficiais
    # Fonte oficial de feeds: https://conteudo.cvm.gov.br/feed.html
    # ──────────────────────────────────────────────
    {
        "id": "cvm_decisoes",
        "name": "CVM — Decisões do Colegiado",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/decisoes.xml",
    },
    {
        "id": "cvm_legislacao",
        "name": "CVM — Legislação",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/legislacao.xml",
    },
    {
        "id": "cvm_sancionadores",
        "name": "CVM — Processos Sancionadores",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/sancionadores.xml",
    },
    {
        "id": "cvm_despachos",
        "name": "CVM — Despachos",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/despachos.xml",
    },
    {
        "id": "cvm_audiencias",
        "name": "CVM — Audiências Públicas",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/audiencias.xml",
    },
    {
        "id": "cvm_informativos",
        "name": "CVM — Informativos do Colegiado",
        "category": "Regulatório",
        "rss_url": "http://www.cvm.gov.br/feed/informativos_colegiado.xml",
    },

    # ──────────────────────────────────────────────
    # Portal Gov.br — RSS oficial por coleções temáticas
    # O portal Gov.br mantém coleções RSS por editoria, incluindo
    # Economia e Gestão Pública e Últimas Notícias.
    # ──────────────────────────────────────────────
    {
        "id": "gov_economia",
        "name": "Gov.br — Economia e Gestão Pública",
        "category": "Fiscal",
        "rss_url": "https://www.gov.br/pt-br/rss/colecoes-de-rss/economia-e-gestao-publica/RSS",
    },
    {
        "id": "gov_ultimas",
        "name": "Gov.br — Últimas Notícias",
        "category": "Fiscal",
        "rss_url": "https://www.gov.br/pt-br/rss/colecoes-de-rss/ultimas-noticias/RSS",
    },

    # ──────────────────────────────────────────────
    # Ministério da Fazenda / Governo Federal
    # Alguns conteúdos mais relevantes do MF acabam aparecendo nas coleções
    # do Gov.br; mantemos o filtro por relevância para segurar o ruído.
    # ──────────────────────────────────────────────
    {
        "id": "gov_financas",
        "name": "Gov.br — Finanças, Impostos e Gestão Pública",
        "category": "Fiscal",
        "rss_url": "https://www.gov.br/pt-br/noticias/financas-impostos-e-gestao-publica/ultimas-noticias/RSS",
    },
]

# Palavras-chave para o filtro inicial de relevância.
# A ideia aqui é ser amplo o suficiente para não perder notícia importante,
# mas sem virar um aspirador de ruído institucional.
RELEVANCE_KEYWORDS = [
    # Política monetária / juros / inflação
    "copom",
    "selic",
    "juros",
    "taxa de juros",
    "política monetária",
    "inflação",
    "ipca",
    "ipca-15",
    "igp-m",
    "igp",
    "focus",
    "boletim focus",

    # Mercado de capitais / regulação
    "cvm",
    "mercado de capitais",
    "oferta pública",
    "oferta",
    "debênture",
    "debêntures",
    "debenture",
    "securitização",
    "fii",
    "fiagro",
    "fundo de investimento",
    "fundos de investimento",
    "fundo",
    "fundos",
    "cri",
    "cra",
    "crás",
    "ipo",
    "follow-on",
    "companhia aberta",
    "valores mobiliários",
    "ações",
    "acao",
    "ações preferenciais",
    "ações ordinárias",

    # Crédito / bancário / financeiro
    "crédito",
    "credito",
    "spread bancário",
    "spread",
    "inadimplência",
    "inadimplencia",
    "sistema financeiro",
    "banco central",
    "bancos",
    "bancário",
    "bancaria",
    "tesouro direto",
    "tesouro nacional",
    "curva de juros",

    # Fiscal / governo / arrecadação
    "fiscal",
    "arcabouço fiscal",
    "arcabouco fiscal",
    "resultado primário",
    "resultado primario",
    "meta fiscal",
    "superávit",
    "superavit",
    "déficit",
    "deficit",
    "arrecadação",
    "arrecadacao",
    "receita federal",
    "ministério da fazenda",
    "ministerio da fazenda",
    "tributário",
    "tributaria",
    "tributário",
    "imposto de renda",
    "iof",
    "pis",
    "cofins",
    "csll",
    "reforma tributária",
    "reforma tributaria",
    "orçamento",
    "orcamento",
    "gasto público",
    "gasto publico",
    "dívida pública",
    "divida publica",

    # Macro / atividade / indicadores
    "ibge",
    "pib",
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
    "massa salarial",
    "balança comercial",
    "balanca comercial",

    # Empresas / resultados / mercado
    "resultado",
    "lucro",
    "prejuízo",
    "prejuizo",
    "ebitda",
    "guidance",
    "dividendos",
    "dividendo",
    "jcp",
    "juros sobre capital próprio",
    "juros sobre capital proprio",
    "margem",
    "receita líquida",
    "receita liquida",
    "captação",
    "captacao",
    "emissão",
    "emissao",

    # Internacional com impacto econômico
    "fed",
    "fomc",
    "bce",
    "ecb",
    "treasury",
    "china",
    "estados unidos",
    "dólar",
    "dolar",
    "câmbio",
    "cambio",
    "commodities",
    "petróleo",
    "petroleo",
]

# Opcional: lista de palavras que ajudam a descartar ruído institucional
# em evoluções futuras do projeto.
IRRELEVANT_HINTS = [
    "aniversário",
    "aniversario",
    "campanha institucional",
    "evento interno",
    "seminário comemorativo",
    "seminario comemorativo",
    "premiação",
    "premiacao",
    "agenda cultural",
]