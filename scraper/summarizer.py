import os
import json
import time
import logging
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


def summarize(title, content, source_name, retries=3):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY nao encontrada.")

    client = anthropic.Anthropic(api_key=api_key)

    user_message = f"""Fonte: {source_name}
Titulo: {title}
Conteudo: {content[:2000]}

Analise esta noticia e retorne o JSON estruturado conforme as instrucoes."""

    for attempt in range(retries):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
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

            return result

        except json.JSONDecodeError as e:
            logger.warning(f"JSON invalido na tentativa {attempt + 1}: {e}")

        except anthropic.RateLimitError:
            wait = 2 ** attempt * 5
            logger.warning(f"Rate limit. Aguardando {wait}s...")
            time.sleep(wait)

        except Exception as e:
            logger.error(f"Erro na tentativa {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(2)

    return None


def estimate_cost(num_articles):
    input_cost = (num_articles * 500 / 1_000_000) * 0.25
    output_cost = (num_articles * 100 / 1_000_000) * 1.25
    total = input_cost + output_cost
    return {
        "articles": num_articles,
        "estimated_usd": round(total, 4),
        "estimated_brl": round(total * 5.0, 3),
    }