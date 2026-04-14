"""
Coletor dedicado para a API do Copom (Banco Central do Brasil).
Busca comunicados e atas diretamente da API de dados abertos do BCB.
Compativel com a arquitetura do main.py — retorna artigos no mesmo formato.
"""

import os
import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# API de dados abertos do BCB — Copom
COPOM_API = "https://dadosabertos.bcb.gov.br/api/3/action/datastore_search"
RESOURCE_ID = "b8cb3545-d51a-4558-b979-0495c5c5650d"

# Base para montar links dos documentos
BCB_BASE = "https://www.bcb.gov.br"


def fetch_copom(max_items: int = 5, max_age_days: int = 90) -> list[dict]:
    """
    Busca os comunicados e atas mais recentes do Copom via API BCB.

    Retorna lista de dicts no mesmo formato que fetch_feed() do main.py:
    [{"title", "url", "content", "published_at", "source_id", "source_name"}]

    max_age_days=90 porque atas e comunicados do Copom saem a cada ~45 dias.
    """
    try:
        params = {
            "resource_id": RESOURCE_ID,
            "limit": max_items * 2,  # pega mais para filtrar por data
            "sort": "Ordem desc",   # mais recentes primeiro
        }
        headers = {"User-Agent": "NewsBot/1.0 (financial news aggregator)"}
        resp = requests.get(COPOM_API, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("result", {}).get("records", [])
        if not records:
            logger.warning("[bcb_copom] Nenhum registro retornado pela API")
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        articles = []

        for rec in records:
            # Tenta extrair data — campo pode variar
            date_str = rec.get("DataPublicacao") or rec.get("Data") or ""
            published_at = None
            if date_str:
                try:
                    # Formato comum: "2025-01-01" ou "01/01/2025"
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
                        try:
                            dt = datetime.strptime(date_str[:10], fmt[:8])
                            dt = dt.replace(tzinfo=timezone.utc)
                            published_at = dt.isoformat()
                            # Descarta se muito antigo
                            if dt < cutoff:
                                continue
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass

            # Tenta montar título
            tipo = rec.get("Tipo", "Documento")
            ordem = rec.get("Ordem", "")
            titulo = f"Copom {ordem} — {tipo}" if ordem else f"Copom — {tipo}"

            # Tenta montar URL do documento
            link_ata = rec.get("LinkAta") or rec.get("UrlAta") or ""
            link_comunicado = rec.get("LinkComunicado") or rec.get("UrlComunicado") or ""
            url = link_ata or link_comunicado or f"{BCB_BASE}/copom/historicotaxasjuros"

            # Monta conteúdo com os campos disponíveis
            taxa = rec.get("MetaSelic") or rec.get("TaxaSelic") or ""
            content_parts = [f"Reuniao do Copom numero {ordem}."]
            if taxa:
                content_parts.append(f"Taxa Selic definida: {taxa}% ao ano.")
            if tipo:
                content_parts.append(f"Tipo de documento: {tipo}.")
            content = " ".join(content_parts)

            articles.append({
                "title": titulo,
                "url": url,
                "content": content,
                "published_at": published_at,
                "source_id": "bcb_copom",
                "source_name": "Banco Central — Copom",
            })

            if len(articles) >= max_items:
                break

        logger.info(f"[bcb_copom] {len(articles)} documentos encontrados")
        return articles

    except Exception as e:
        logger.error(f"[bcb_copom] Erro ao buscar API: {e}")
        return []