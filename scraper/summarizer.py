import os
import json
import time
import logging
import re
import anthropic

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Voce e um analista financeiro senior especializado no mercado brasileiro.
Sua tarefa e analisar noticias e criar resumos imparciais e objetivos.

Regras obrigatorias:
- Seja 100% factual, sem opiniao, sem vies, sem especulacao
- Use linguagem tecnica mas acessivel
- Foque em numeros, datas e impactos concretos quando disponiveis
- O resumo deve ter entre 2 e 4 frases completas

Retorne SOMENTE um JSON valido, sem texto antes ou depois, sem markdown:
{
  "resumo": "Resumo imparcial de 2-4 frases.",
  "categoria": "UMA das opcoes: Politica Monetaria | Resultado Financeiro | Regulatorio | Fiscal | Operacional | Internacional | Mercado de Capitais | ESG | Legislativo | Judiciario",
  "relevancia": "alta | media | baixa",
  "keywords": ["palavra1", "palavra2", "palavra3"]
}"""

VALID_CATEGORIES = {
    "Politica Monetaria",
    "Resultado Financeiro",
    "Regulatorio",
    "Fiscal",
    "Operacional",
    "Internacional",
    "Mercado de Capitais",
    "ESG",
    "Legislativo",
    "Judiciario",
}

VALID_RELEVANCE = {"alta", "media", "baixa"}


def _clean_text(text):
    text = str(text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_relevance(value):
    value = _clean_text(value).lower()
    value = (
        value.replace("média", "media")
        .replace("médio", "media")
        .replace("média ", "media")
    )
    if value not in VALID_RELEVANCE:
        return "media"
    return value


def _infer_category(title, content):
    text = f"{title} {content}".lower()

    if any(x in text for x in ["copom", "selic", "juros", "ipca", "inflacao", "inflação", "focus"]):
        return "Politica Monetaria"
    if any(x in text for x in ["cvm", "regulacao", "regulação", "norma", "resolucao", "resolução"]):
        return "Regulatorio"
    if any(x in text for x in ["fiscal", "arrecadacao", "arrecadação", "orcamento", "orçamento", "tribut", "imposto"]):
        return "Fiscal"
    if any(x in text for x in ["lucro", "prejuizo", "prejuízo", "ebitda", "resultado", "balanco", "balanço"]):
        return "Resultado Financeiro"
    if any(x in text for x in ["ipo", "debenture", "debênture", "fundo", "mercado de capitais", "acoes", "ações"]):
        return "Mercado de Capitais"
    if any(x in text for x in ["china", "eua", "fed", "bce", "fomc", "internacional", "dolar", "dólar"]):
        return "Internacional"
    if any(x in text for x in ["ibge", "pib", "varejo", "servicos", "serviços", "producao", "produção"]):
        return "Operacional"

    return "Operacional"


def _infer_relevance(title, content):
    text = f"{title} {content}".lower()

    strong_terms = [
        "copom", "selic", "ipca", "pib", "varejo", "servicos", "serviços",
        "producao industrial", "produção industrial", "fiscal", "debenture", "debênture"
    ]
    if any(term in text for term in strong_terms):
        return "alta"

    return "media"


def _infer_keywords(title, content, limit=5):
    text = f"{title} {content}".lower()
    candidates = [
        "copom", "selic", "ipca", "focus", "ibge", "pib", "varejo",
        "servicos", "serviços", "producao industrial", "produção industrial",
        "fiscal", "debenture", "debênture", "cvm", "dolar", "dólar"
    ]
    found = []
    for term in candidates:
        if term in text and term not in found:
            found.append(term)
    return found[:limit]


def _fallback_summary(title, content, source_name):
    clean_title = _clean_text(title)
    clean_content = _clean_text(content)

    if clean_content:
        resumo = clean_content[:280]
        if len(clean_content) > 280:
            resumo += "..."
    else:
        resumo = clean_title

    return {
        "resumo": resumo,
        "categoria": _infer_category(clean_title, clean_content),
        "relevancia": _infer_relevance(clean_title, clean_content),
        "keywords": _infer_keywords(clean_title, clean_content),
    }


def summarize(title, content, source_name, retries=3):
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if not api_key:
        logger.error("ANTHROPIC_API_KEY nao encontrada. Usando fallback local.")
        return _fallback_summary(title, content, source_name)

    try:
        client = anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        logger.error("Falha ao inicializar cliente Anthropic: %s", e)
        return _fallback_summary(title, content, source_name)

    user_message = f"""Fonte: {source_name}
Titulo: {title}
Conteudo: {content[:2000]}

Analise esta noticia e retorne o JSON estruturado conforme as instrucoes."""

    for attempt in range(retries):
        try:
            response = client.messages.create(
                model="claude-3-5-haiku-latest",
                max_tokens=400,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            raw = response.content[0].text.strip()

            if raw.startswith("```"):
                parts = raw.split("```")
                if len(parts) > 1:
                    raw = parts[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()

            if not (raw.startswith("{") and raw.endswith("}")):
                start = raw.find("{")
                end = raw.rfind("}")
                if start != -1 and end != -1:
                    raw = raw[start:end + 1]

            result = json.loads(raw)

            required = {"resumo", "categoria", "relevancia", "keywords"}
            if not required.issubset(result.keys()):
                raise ValueError(f"JSON incompleto: {result}")

            categoria = _clean_text(result.get("categoria"))
            if categoria not in VALID_CATEGORIES:
                categoria = _infer_category(title, content)

            relevancia = _normalize_relevance(result.get("relevancia"))

            keywords = result.get("keywords", [])
            if not isinstance(keywords, list):
                keywords = _infer_keywords(title, content)

            return {
                "resumo": _clean_text(result.get("resumo")) or _clean_text(title),
                "categoria": categoria,
                "relevancia": relevancia,
                "keywords": [_clean_text(k) for k in keywords if _clean_text(k)][:5],
            }

        except json.JSONDecodeError as e:
            logger.warning("JSON invalido na tentativa %d: %s", attempt + 1, e)

        except anthropic.RateLimitError:
            wait = (2 ** attempt) * 5
            logger.warning("Rate limit. Aguardando %ss...", wait)
            time.sleep(wait)

        except anthropic.AuthenticationError as e:
            logger.error("Erro de autenticacao Anthropic: %s", e)
            return _fallback_summary(title, content, source_name)

        except anthropic.BadRequestError as e:
            logger.error("Erro de requisicao Anthropic: %s", e)
            return _fallback_summary(title, content, source_name)

        except Exception as e:
            logger.error("Erro na tentativa %d: %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(2)

    logger.error("Falha ao resumir apos %d tentativas. Usando fallback local.", retries)
    return _fallback_summary(title, content, source_name)


def estimate_cost(num_articles):
    input_cost = (num_articles * 500 / 1_000_000) * 0.25
    output_cost = (num_articles * 100 / 1_000_000) * 1.25
    total = input_cost + output_cost
    return {
        "articles": num_articles,
        "estimated_usd": round(total, 4),
        "estimated_brl": round(total * 5.0, 3),
    }