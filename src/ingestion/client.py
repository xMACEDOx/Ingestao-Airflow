"""
client.py
─────────
Responsabilidade única: falar com a API NeoWs da NASA.

O que faz:
    - Monta a URL com os parâmetros corretos
    - Faz o GET com timeout configurável
    - Retry automático em erros 429 (rate limit) e 5xx (servidor)
    - Retorna o dict Python do JSON — sem modificar nada

O que NÃO faz:
    - Não salva arquivo nenhum
    - Não lê campos dentro do JSON retornado
    - Não sabe nada sobre MinIO ou camadas bronze/silver

Uso standalone (para testar sem Airflow):
    from src.ingestion.client import NasaClient
    client = NasaClient()
    data = client.fetch_feed("2024-01-01", "2024-01-07")
"""

import time
import logging

import requests

from src.ingestion import config

logger = logging.getLogger(__name__)


class NasaApiError(Exception):
    """Erro não recuperável da API da NASA (ex: 400, 403, 404)."""
    pass


class NasaRateLimitError(Exception):
    """Rate limit atingido e retries esgotados."""
    pass


class NasaClient:
    """
    Cliente HTTP para a API NeoWs da NASA.

    Exemplo:
        client = NasaClient()
        payload = client.fetch_feed("2024-01-01", "2024-01-07")
        # payload é o dict Python — exatamente o que a NASA enviou
    """

    def __init__(
        self,
        api_key:     str = None,
        base_url:    str = None,
        timeout:     int = None,
        max_retries: int = None,
        backoff_base: int = None,
    ):
        self.api_key      = api_key      or config.NASA_API_KEY
        self.base_url     = base_url     or config.NASA_BASE_URL
        self.timeout      = timeout      or config.NASA_REQUEST_TIMEOUT
        self.max_retries  = max_retries  or config.NASA_MAX_RETRIES
        self.backoff_base = backoff_base or config.NASA_BACKOFF_BASE

        self.session = requests.Session()

    def fetch_feed(self, start_date: str, end_date: str) -> dict:
        """
        Busca asteroides próximos à Terra em uma janela de datas.

        Args:
            start_date: Data inicial no formato YYYY-MM-DD (ex: "2024-01-01")
            end_date:   Data final no formato YYYY-MM-DD   (ex: "2024-01-07")
                        Máximo 7 dias de diferença (limite da NASA API)

        Returns:
            dict: payload JSON exatamente como retornado pela NASA

        Raises:
            NasaApiError:       erro não recuperável (4xx exceto 429)
            NasaRateLimitError: rate limit atingido após todos os retries
            requests.Timeout:   timeout após todos os retries
        """
        params = {
            "start_date": start_date,
            "end_date":   end_date,
            "api_key":    self.api_key,
        }

        for attempt in range(1, self.max_retries + 1):
            logger.info(
                "Buscando feed NASA | start=%s end=%s | tentativa %d/%d",
                start_date, end_date, attempt, self.max_retries
            )

            try:
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout,
                )

                # ── Sucesso ──────────────────────────────────
                if response.status_code == 200:
                    logger.info(
                        "Feed recebido com sucesso | start=%s end=%s | %d bytes",
                        start_date, end_date, len(response.content)
                    )
                    return response.json()

                # ── Rate limit (429) — tenta de novo ─────────
                if response.status_code == 429:
                    wait = self.backoff_base ** attempt
                    logger.warning(
                        "Rate limit atingido (429) | aguardando %ds antes da tentativa %d",
                        wait, attempt + 1
                    )
                    if attempt == self.max_retries:
                        raise NasaRateLimitError(
                            f"Rate limit da NASA atingido após {self.max_retries} tentativas. "
                            f"Janela: {start_date} → {end_date}"
                        )
                    time.sleep(wait)
                    continue

                # ── Erro de servidor (5xx) — tenta de novo ───
                if response.status_code >= 500:
                    wait = self.backoff_base ** attempt
                    logger.warning(
                        "Erro de servidor NASA (%d) | aguardando %ds antes da tentativa %d",
                        response.status_code, wait, attempt + 1
                    )
                    if attempt == self.max_retries:
                        response.raise_for_status()
                    time.sleep(wait)
                    continue

                # ── Outros erros (4xx) — falha imediata ──────
                logger.error(
                    "Erro não recuperável da NASA API | status=%d | body=%s",
                    response.status_code, response.text[:200]
                )
                raise NasaApiError(
                    f"NASA API retornou {response.status_code} para "
                    f"{start_date} → {end_date}: {response.text[:200]}"
                )

            except requests.Timeout:
                wait = self.backoff_base ** attempt
                logger.warning(
                    "Timeout na requisição | aguardando %ds antes da tentativa %d",
                    wait, attempt + 1
                )
                if attempt == self.max_retries:
                    raise
                time.sleep(wait)

        # Nunca deve chegar aqui, mas por segurança:
        raise NasaApiError(f"Falha após {self.max_retries} tentativas: {start_date} → {end_date}")
