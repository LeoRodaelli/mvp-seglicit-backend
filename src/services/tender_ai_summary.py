# -*- coding: utf-8 -*-
"""
Resumo de licitação por IA (Claude). Tenta resumir a partir do PDF real do
edital quando disponível (Claude lê PDF nativamente, sem precisar de
extração de texto à parte) — cai pro texto curto já salvo no banco
(objeto/descrição) quando não tem PDF ou o download falha.

O resumo é gerado sob demanda (clique do usuário) e cacheado em
`tenders.ai_summary` — nunca gera de novo pra quem já tem o resumo salvo.
"""
import base64
import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

MODEL_ID = 'claude-opus-5-5'
MAX_PDF_BYTES = 15 * 1024 * 1024  # ~15MB — margem segura sob o limite de 32MB da API (base64 soma ~33%)

SUMMARY_SYSTEM_PROMPT = (
    "Você resume editais de licitações públicas brasileiras para empresas que "
    "avaliam se vale a pena participar. Seja objetivo e factual — nunca invente "
    "informação que não está no documento/texto fornecido. Se um dado não "
    "aparecer, diga \"não informado\" em vez de adivinhar. Responda em "
    "português do Brasil, em tópicos curtos, sem introdução nem conclusão — "
    "só o resumo direto, pronto para leitura rápida. Estruture assim:\n\n"
    "**Prazo de propostas:** ...\n"
    "**Valor estimado:** ...\n"
    "**Modalidade:** ...\n"
    "**Principais exigências/documentos:** ...\n"
    "**Pontos de atenção:** ... (ex: garantias exigidas, penalidades incomuns, "
    "prazos de execução apertados — só se houver algo relevante)"
)


def _find_pdf_url(downloaded_files_json):
    """Pega a URL do primeiro arquivo PDF salvo pro edital, se houver."""
    if not downloaded_files_json:
        return None
    try:
        files = json.loads(downloaded_files_json) if isinstance(downloaded_files_json, str) else downloaded_files_json
    except Exception:
        return None
    for f in files or []:
        url = (f or {}).get('url')
        filename = (f or {}).get('filename', '') or ''
        if url and (filename.lower().endswith('.pdf') or '.pdf' in url.lower() or not filename):
            return url
    return None


def _fetch_pdf_base64(url):
    """Baixa o PDF e devolve em base64, ou None se falhar/for grande demais."""
    try:
        resp = requests.get(url, timeout=30, stream=True)
        if resp.status_code != 200:
            logger.warning(f"Resumo IA: PDF retornou status {resp.status_code} ({url})")
            return None

        content = bytearray()
        for chunk in resp.iter_content(chunk_size=262144):
            content.extend(chunk)
            if len(content) > MAX_PDF_BYTES:
                logger.warning(f"Resumo IA: PDF excedeu {MAX_PDF_BYTES} bytes, abortando download ({url})")
                return None

        if not content:
            return None

        return base64.standard_b64encode(bytes(content)).decode('utf-8')
    except Exception as exc:
        logger.warning(f"Resumo IA: erro ao baixar PDF ({url}): {exc}")
        return None


def generate_tender_summary(tender):
    """
    Gera o resumo por IA de uma licitação (dict com campos de `tenders`).
    Retorna (summary_text, source) onde source é 'pdf' ou 'texto'.
    Lança exceção se a chamada à Claude falhar — o chamador decide como tratar.
    """
    import anthropic

    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise RuntimeError('ANTHROPIC_API_KEY não configurada')

    client = anthropic.Anthropic(api_key=api_key)

    pdf_url = _find_pdf_url(tender.get('downloaded_files_json'))
    pdf_b64 = _fetch_pdf_base64(pdf_url) if pdf_url else None

    if pdf_b64:
        content = [
            {
                'type': 'document',
                'source': {
                    'type': 'base64',
                    'media_type': 'application/pdf',
                    'data': pdf_b64,
                },
            },
            {
                'type': 'text',
                'text': 'Resuma o edital anexo seguindo exatamente a estrutura pedida.',
            },
        ]
        source = 'pdf'
    else:
        texto = '\n\n'.join(filter(None, [
            f"Título: {tender.get('title')}" if tender.get('title') else None,
            f"Objeto: {tender.get('objeto')}" if tender.get('objeto') else None,
            f"Descrição: {tender.get('description')}" if tender.get('description') else None,
            f"Descrição detalhada: {tender.get('detailed_description')}" if tender.get('detailed_description') else None,
        ]))
        if not texto.strip():
            raise ValueError('Sem conteúdo suficiente (nem PDF, nem texto) para resumir esta licitação')
        content = (
            "Resuma a licitação abaixo seguindo exatamente a estrutura pedida. "
            "Atenção: isto é só o resumo curto do PNCP, não o edital completo — "
            "se algum dado pedido não aparecer aqui, diga \"não informado\" em "
            "vez de adivinhar.\n\n" + texto
        )
        source = 'texto'

    response = client.messages.create(
        model=MODEL_ID,
        max_tokens=1024,
        system=SUMMARY_SYSTEM_PROMPT,
        messages=[{'role': 'user', 'content': content}],
    )

    summary = ''.join(block.text for block in response.content if block.type == 'text').strip()
    if not summary:
        raise RuntimeError('Claude retornou resposta vazia')

    return summary, source
