"""
run_ingestion.py
────────────────
Entrypoint CLI para rodar a ingestão sem precisar do Airflow.

Uso:
    # Ingestão de uma janela específica
    python run_ingestion.py --start 2024-01-01 --end 2024-01-31

    # Ingestão do dia de ontem (padrão para rodar manualmente)
    python run_ingestion.py

    # Ver ajuda
    python run_ingestion.py --help

Por que isso existe?
    Permite testar e debugar o pipeline sem depender do Airflow.
    O DAG chama a mesma função run() — o comportamento é idêntico.
"""

import argparse
import json
import logging
import sys
from datetime import date, timedelta

# Configura logging para output legível no terminal
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)

from src.ingestion.pipeline import run


def parse_args():
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="Ingestão de asteroides NASA NeoWs → MinIO Bronze"
    )
    parser.add_argument(
        "--start",
        default=yesterday,
        help=f"Data inicial YYYY-MM-DD (padrão: ontem = {yesterday})",
    )
    parser.add_argument(
        "--end",
        default=yesterday,
        help=f"Data final YYYY-MM-DD (padrão: ontem = {yesterday})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print(f"\n{'─'*50}")
    print(f"  NASA NeoWs Ingestion")
    print(f"  Período: {args.start} → {args.end}")
    print(f"{'─'*50}\n")

    summary = run(start_date=args.start, end_date=args.end)

    print(f"\n{'─'*50}")
    print(f"  Sumário da execução")
    print(f"{'─'*50}")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    # Exit code 1 se houve qualquer erro — útil para CI/CD
    if summary["errors"] > 0:
        print(f"\n⚠️  {summary['errors']} janela(s) falharam. Verifique os logs acima.")
        sys.exit(1)

    print(f"\n✅ Ingestão concluída com sucesso.")
