"""
airflow/dags/nasa_neows_ingestion.py
─────────────────────────────────────
DAG de ingestão diária dos dados de asteroides NASA NeoWs.

Responsabilidade do DAG:
    - Definir o schedule (diário)
    - Definir retry e alertas
    - Chamar o pipeline.run() com a data de execução correta

O que o DAG NÃO faz:
    - Não contém lógica de negócio
    - Não sabe nada de HTTP ou MinIO
    - Toda a lógica está em src/ingestion/pipeline.py

Schedule:
    Roda todo dia às 06:00 UTC, ingerindo os dados do dia anterior.
    (A NASA pode demorar algumas horas para disponibilizar o dia atual)

Como testar manualmente:
    1. Abrir Airflow em localhost:8080
    2. Ativar a DAG "nasa_neows_ingestion"
    3. Clicar em "Trigger DAG w/ config" e passar:
       {"start_date": "2024-01-01", "end_date": "2024-01-07"}
    4. Verificar o arquivo em MinIO > bronze > neows > ...
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Garante que src/ está no path do Python dentro do container Airflow
# (o docker-compose monta ./src em /opt/airflow/src)
import sys, os
sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)


# ── Argumentos padrão aplicados a todas as tasks ─────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,   # altere para True e configure SMTP em produção
    "email_on_retry":   False,
}


# ── Função executada pela task ────────────────────────────────────
def ingest_neows(**context) -> dict:
    """
    Função chamada pelo PythonOperator.

    Determina a janela de datas a partir do contexto do Airflow:
    - Em execução normal: ingere o dia anterior ao data_interval_end
    - Em execução manual com config: usa start_date/end_date do config

    Args:
        context: contexto injetado pelo Airflow (datas, dag_run, etc.)

    Returns:
        dict com sumário da execução (salvo nos XComs do Airflow)
    """
    # Import aqui dentro para isolar do escopo do DAG
    # e facilitar testes unitários da função
    from src.ingestion.pipeline import run

    dag_run = context.get("dag_run")

    # Execução manual com configuração explícita
    if dag_run and dag_run.conf:
        start_date = dag_run.conf.get("start_date")
        end_date   = dag_run.conf.get("end_date")

        if start_date and end_date:
            log.info("Execução manual | start=%s end=%s", start_date, end_date)
            return run(start_date=start_date, end_date=end_date)

    # Execução agendada: pega o dia anterior ao fim do intervalo
    # data_interval_end é o momento "lógico" do final do período
    data_interval_end = context["data_interval_end"]
    yesterday = (data_interval_end - timedelta(days=1)).date()

    start_date = yesterday.isoformat()
    end_date   = yesterday.isoformat()

    log.info("Execução agendada | start=%s end=%s", start_date, end_date)

    summary = run(start_date=start_date, end_date=end_date)

    # Falha a task se houve erros de ingestão
    if summary["errors"] > 0:
        raise RuntimeError(
            f"Ingestão concluída com {summary['errors']} erro(s). "
            f"Verifique os logs para detalhes."
        )

    return summary


# ── Definição da DAG ──────────────────────────────────────────────
with DAG(
    dag_id      = "nasa_neows_ingestion",
    description = "Ingestão diária de asteroides NASA NeoWs → MinIO Bronze",
    schedule    = "0 6 * * *",      # todo dia às 06:00 UTC
    start_date  = datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup     = False,            # não reprocessa datas passadas ao ativar
    max_active_runs = 1,            # evita execuções paralelas da mesma DAG
    tags        = ["nasa", "ingestion", "bronze", "neows"],
    default_args= default_args,
    doc_md      = """
## NASA NeoWs — Ingestão Diária

Busca diariamente os asteroides próximos à Terra via NASA NeoWs API
e salva o JSON bruto no bucket **bronze** do MinIO.

### Caminho no MinIO
```
bronze/neows/year=YYYY/month=MM/day=DD.json
```

### Execução manual
Para ingerir um período específico, use "Trigger DAG w/ config":
```json
{
  "start_date": "2024-01-01",
  "end_date": "2024-01-31"
}
```

### Próxima etapa
Após a ingestão, os dados são processados pela DAG `nasa_neows_transform`
que normaliza o JSON e carrega na camada silver.
    """,
) as dag:

    ingest_task = PythonOperator(
        task_id         = "ingest_neows_to_bronze",
        python_callable = ingest_neows,
        doc_md          = """
### ingest_neows_to_bronze

Chama `src.ingestion.pipeline.run()` com a data do dia anterior.

**Sucesso:** JSON salvo em `bronze/neows/year=.../month=.../day=....json`
**Idempotente:** re-executar a mesma data não duplica o arquivo.
        """,
    )

    # Estrutura atual: 1 task
    # Próxima etapa: adicionar task de validação após a ingestão
    ingest_task
