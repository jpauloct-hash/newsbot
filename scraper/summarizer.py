"""
Módulo de resumo usando a API do Claude.
Envia o texto da notícia e recebe um resumo estruturado em JSON.
"""

import os
import json
import time
import logging
import anthropic

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Você é um analista financeiro sênior especializado no mercado brasileiro.
Sua tarefa é analisar notícias e criar resumos imparciais e objetivos.

Regras obrigatórias:
- Seja 100% factual — sem opinião, sem viés, sem especulação
- Use linguagem técnica mas acessível
- Foque em números, datas e impactos concretos quando disponíveis
- O resumo deve ter entre 2 e 4 frases completas

Retorne SOMENTE um JSON válido, sem texto antes ou depois, sem markdown:
{
  "resumo": "Resumo imparcial de 2-4 frases.",
  "categoria": "UMA das opções: Política Monetária | Resultado Financeiro | Regulatório | Fiscal | Operacional | Internacional | Mercado de Capitais | ESG | Legislativo | Judiciário",
  "relevancia": "alta | média | baixa",
  "keywords": ["palavra1", "palavra2", "palavra3"]
}
"""


def summarize(title: str, content: str, source_name: str, retries: int = 3) -> dict | None:
    """
    Envia uma notícia para o Claude e retorna o resumo estruturado.

    Args:
        title: Título da notícia
        content: Texto/descrição da notícia
        source_name: Nome da fonte (para contexto)
        retries: Número de tentativas em caso de falha

    Returns:
        dict com resumo, categoria, relevância e keywords — ou None em caso de erro
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY não encontrada nas variáveis de ambiente.")

    client = anthropic.Anthropic(api_key=api_key)

    user_message = f"""Fonte: {source_name}
Título: {title}
Conteúdo: {content[:2000]}

Analise esta notícia e retorne o JSON estruturado conforme as instruções."""

    for attempt in range(retries):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            raw = response.content[0].text.strip()

            # Remove markdown ```json
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()

            result = json.loads(raw)

            # Validação
            required = {"resumo", "categoria", "relevancia", "keywords"}
            if not required.issubset(result.keys()):
                raise ValueError(f"JSON incompleto: {result}")

            return result

        except json.JSONDecodeError as e:
            logger.warning(f"JSON inválido na tentativa {attempt + 1}: {e}")

        except anthropic.RateLimitError:
            wait = 2 ** attempt * 5
            logger.warning(f"Rate limit atingido. Aguardando {wait}s...")
            time.sleep(wait)

        except Exception as e:
            logger.error(f"Erro na tentativa {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(2)

    return None


def estimate_cost(num_articles: int) -> dict:
    """
    Estima o custo aproximado de uma execução.
    """
    input_cost = (num_articles * 500 / 1_000_000) * 0.25
    output_cost = (num_articles * 100 / 1_000_000) * 1.25
    total = input_cost + output_cost

    return {
        "articles": num_articles,
        "estimated_usd": round(total, 4),
        "estimated_brl": round(total * 5.0, 3),
    }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        }