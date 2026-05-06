"""
pipeline.py
───────────
Orquestra client.py e storage.py.

Responsabilidades:
    - Receber um intervalo de datas (ex: últimos 30 dias)
    - Quebrar em janelas de 7 dias (limite da NASA API)
    - Para cada janela: buscar via NasaClient → salvar via MinioStorage
    - Retornar sumário da execução

O que NÃO faz:
    - Não importa nada do Airflow — é agnóstico ao orquestrador
    - Não transforma ou lê o conteúdo do JSON
    - Não toma decisões de negócio — só coordena

Uso standalone (linha de comando):
    python run_ingestion.py --start 2024-01-01 --end 2024-01-31

Uso pelo DAG do Airflow:
    from src.ingestion.pipeline import run
    run(start_date="2024-01-01", end_date="2024-01-31")
"""

import logging
from datetime import date, timedelta
from typing import Generator

from src.ingestion import config
from src.ingestion.client import NasaClient
from src.ingestion.storage import MinioStorage

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
#  Funções puras — fáceis de testar unitariamente
# ─────────────────────────────────────────────────────────────────

def generate_windows(
    start: date,
    end: date,
    window_days: int = None,
) -> Generator[tuple[date, date], None, None]:
    """
    Quebra um intervalo de datas em janelas de N dias.

    A NASA NeoWs aceita no máximo 7 dias por requisição.
    Esta função garante que nunca ultrapassamos esse limite.

    Args:
        start:       data inicial (inclusive)
        end:         data final (inclusive)
        window_days: tamanho da janela (padrão: NASA_MAX_WINDOW_DAYS = 7)

    Yields:
        tuplas (window_start, window_end) onde window_end <= end

    Exemplo:
        generate_windows(date(2024,1,1), date(2024,1,20))
        → (2024-01-01, 2024-01-07)
        → (2024-01-08, 2024-01-14)
        → (2024-01-15, 2024-01-20)  ← última janela pode ser menor
    """
    max_days = window_days or config.NASA_MAX_WINDOW_DAYS
    current  = start

    while current <= end:
        window_end = min(current + timedelta(days=max_days - 1), end)
        yield current, window_end
        current = window_end + timedelta(days=1)


def build_object_key(window_start: date) -> str:
    """
    Monta o caminho do objeto no bucket bronze.

    Usa particionamento por ano/mês/dia — padrão Hive.
    Facilita leitura por data em ferramentas como Spark e Athena.

    Exemplo:
        build_object_key(date(2024, 1, 15))
        → "neows/year=2024/month=01/day=15.json"
    """
    return (
        f"{config.BRONZE_PREFIX}/"
        f"year={window_start.year}/"
        f"month={window_start.month:02d}/"
        f"day={window_start.day:02d}.json"
    )


# ─────────────────────────────────────────────────────────────────
#  Função principal — chamada pelo DAG e pelo CLI
# ─────────────────────────────────────────────────────────────────

def run(
    start_date: str,
    end_date:   str,
    client:     NasaClient   = None,
    storage:    MinioStorage  = None,
) -> dict:
    """
    Executa a ingestão completa para um intervalo de datas.

    Aceita client e storage como parâmetros opcionais para
    facilitar testes (injeção de dependência com mocks).

    Args:
        start_date: "YYYY-MM-DD" — data inicial
        end_date:   "YYYY-MM-DD" — data final
        client:     NasaClient (opcional — instancia o padrão se None)
        storage:    MinioStorage (opcional — instancia o padrão se None)

    Returns:
        dict com sumário:
            start_date, end_date
            total_windows:  número de janelas processadas
            saved:          arquivos novos enviados ao MinIO
            skipped:        arquivos já existentes (idempotência)
            errors:         janelas que falharam
            results:        lista com resultado de cada janela
    """
    start = date.fromisoformat(start_date)
    end   = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(f"start_date ({start_date}) não pode ser maior que end_date ({end_date})")

    # Instancia com defaults se não foram injetados (útil para testes)
    _client  = client  or NasaClient()
    _storage = storage or MinioStorage()

    logger.info(
        "Iniciando ingestão | start=%s end=%s",
        start_date, end_date
    )

    summary = {
        "start_date":    start_date,
        "end_date":      end_date,
        "total_windows": 0,
        "saved":         0,
        "skipped":       0,
        "errors":        0,
        "results":       [],
    }

    for window_start, window_end in generate_windows(start, end):
        summary["total_windows"] += 1
        ws = window_start.isoformat()
        we = window_end.isoformat()

        logger.info("Processando janela %s → %s", ws, we)

        try:
            # 1. Busca na API
            payload = _client.fetch_feed(ws, we)

            # 2. Monta chave e salva no MinIO
            object_key = build_object_key(window_start)
            result     = _storage.save(data=payload, object_key=object_key)

            # 3. Acumula no sumário
            result["window_start"] = ws
            result["window_end"]   = we
            summary["results"].append(result)

            if result["status"] == "saved":
                summary["saved"] += 1
            else:
                summary["skipped"] += 1

        except Exception as exc:
            logger.error(
                "Erro na janela %s → %s | %s: %s",
                ws, we, type(exc).__name__, exc
            )
            summary["errors"] += 1
            summary["results"].append({
                "status":       "error",
                "window_start": ws,
                "window_end":   we,
                "error":        str(exc),
            })

    logger.info(
        "Ingestão concluída | windows=%d saved=%d skipped=%d errors=%d",
        summary["total_windows"],
        summary["saved"],
        summary["skipped"],
        summary["errors"],
    )

    return summary
