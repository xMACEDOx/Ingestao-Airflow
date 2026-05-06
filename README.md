# NASA NeoWs Ingestion Pipeline

> Pipeline de ingestão de dados astronômicos em ambiente local com Apache Airflow e MinIO — arquitetura medallion bronze/silver/gold, orquestração diária automatizada e código testável e portável para AWS S3 em produção.

---

##  Índice

- [Estudo de Caso](#-estudo-de-caso)
- [Visão Geral da Arquitetura](#-visão-geral-da-arquitetura)
- [Stack de Tecnologias](#-stack-de-tecnologias)
- [Estrutura do Repositório](#-estrutura-do-repositório)
- [Pré-requisitos](#-pré-requisitos)
- [Configuração do Ambiente](#-configuração-do-ambiente)
- [Subindo a Infraestrutura](#-subindo-a-infraestrutura)
- [Código de Ingestão](#-código-de-ingestão)
- [Executando os Testes](#-executando-os-testes)
- [Acionando a DAG](#-acionando-a-dag)
- [Validando a Ingestão](#-validando-a-ingestão)
- [Decisões de Arquitetura](#-decisões-de-arquitetura)
- [Próximos Passos](#-próximos-passos)

---

##  Estudo de Caso

### O Problema de Negócio

Organizações que dependem de dados externos — sejam APIs públicas, parceiros ou fornecedores — enfrentam um problema comum: **como garantir que o dado chegue de forma confiável, rastreável e reutilizável?**

A ingestão sem estrutura gera dívida técnica imediata:

- Dados transformados diretamente na origem, sem camada de raw
- Reprocessamento impossível quando a transformação tem bug
- Sem rastreabilidade de quando e como o dado chegou
- Código acoplado ao orquestrador, impossível de testar isoladamente
- Credenciais expostas no código ou em logs

Esses problemas aparecem em qualquer setor — fintech ingerindo dados de bolsa, healthtech consumindo APIs de prontuário, ou startups de logística integrando rastreadores externos.

### O Cenário

Uma equipe de dados recebeu a missão de construir um **data lake** sobre objetos próximos à Terra (NEOs — Near Earth Objects) usando a NASA NeoWs API. O objetivo final é gerar dashboards de risco astronômico, análises de velocidade e distância de asteroides, e alertas para eventos de aproximação relevantes.

O engenheiro de dados júnior ficou responsável pela **camada de ingestão** — a fundação de todo o pipeline. Um engenheiro sênior revisará o Pull Request antes de qualquer etapa de transformação começar.

### Os Requisitos do Negócio

| Requisito | Descrição |
|-----------|-----------|
| **Confiabilidade** | O pipeline não pode perder dados em caso de falha temporária da API |
| **Rastreabilidade** | Toda ingestão deve ser auditável — quando rodou, o que foi salvo, quantos bytes |
| **Idempotência** | Re-executar o pipeline no mesmo dia não pode duplicar dados |
| **Portabilidade** | O código local deve funcionar em produção na AWS sem reescrita |
| **Testabilidade** | O código deve ser testável sem depender de infraestrutura real |
| **Segurança** | Nenhuma credencial pode aparecer no repositório ou em logs |

### A Solução

Pipeline ELT com **arquitetura medallion** (bronze/silver/gold), orquestrado pelo Apache Airflow com schedule diário, armazenando dados no MinIO — um object storage S3-compatible que replica exatamente o comportamento do AWS S3 em ambiente local.

```
NASA NeoWs API
      │
      ▼
  NasaClient          ← retry automático, backoff exponencial
      │
      ▼
MinioStorage          ← idempotência via head_object antes do upload
      │
      ▼
bronze/neows/         ← JSON bruto particionado por data
  year=2025/
    month=01/
      day=15.json
```

### O que este projeto demonstra

Para recrutadores e líderes técnicos, este repositório evidencia:

- **Princípio da responsabilidade única** — cada módulo tem uma razão para existir e uma razão para mudar
- **Separação entre orquestrador e lógica de negócio** — o DAG é uma casca fina; toda a lógica está nos módulos Python testáveis
- **Injeção de dependência** — client e storage são injetáveis, permitindo testes com mocks sem infra
- **Tratamento defensivo de erros** — retry com backoff exponencial, exceções tipadas, logs estruturados
- **Idempotência por design** — não é um afterthought, é parte da interface do storage
- **Particionamento Hive-style** — `year=/month=/day=` compatível com Spark, Athena e qualquer engine analítica
- **Portabilidade AWS** — trocar MinIO por S3 em produção requer mudar apenas o endpoint nas configs

---

##  Visão Geral da Arquitetura
```
┌─────────────────────────────────────────────────────────┐
│                    Docker Network                        │
│                                                         │
│  ┌──────────┐    ┌──────────────────────────────────┐  │
│  │          │    │           Airflow                │  │
│  │  MinIO   │◄───│  Scheduler │ Webserver           │  │
│  │          │    │  DAG: nasa_neows_ingestion       │  │
│  │ :9000    │    │  Schedule: 0 6 * * * (diário)    │  │
│  │ :9001    │    └──────────────────────────────────┘  │
│  │          │                    │                      │
│  │ bronze/  │    ┌───────────────▼──────────────────┐  │
│  │ silver/  │    │         src/ingestion/            │  │
│  │ gold/    │    │  config.py  ← variáveis .env      │  │
│  │          │    │  client.py  ← GET NASA API        │  │
│  └──────────┘    │  storage.py ← upload boto3        │  │
│                  │  pipeline.py← orquestra os dois   │  │
│  ┌──────────┐    └──────────────────────────────────┘  │
│  │Postgres  │                                           │
│  │(metadata)│    Airflow metadata — não armazena        │
│  │ :5432    │    dado do pipeline, só estado das DAGs   │
│  └──────────┘                                           │
└─────────────────────────────────────────────────────────┘
```

### Fluxo de execução

```
Airflow scheduler (06:00 UTC)
         │
         ▼
  ingest_neows_to_bronze (PythonOperator)
         │
         ▼
  pipeline.run(start_date, end_date)
         │
         ├── generate_windows()     → quebra em janelas de 7 dias
         │
         └── para cada janela:
               │
               ├── NasaClient.fetch_feed()   → GET /neo/rest/v1/feed
               │         │
               │         └── retry em 429/5xx com backoff exponencial
               │
               └── MinioStorage.save()       → upload para bronze/
                         │
                         └── head_object primeiro (idempotência)
```

---

##  Stack de Tecnologias

| Tecnologia | Versão | Papel |
|-----------|--------|-------|
| **Apache Airflow** | 2.10.5 | Orquestração e agendamento do pipeline |
| **MinIO** | latest | Object storage S3-compatible (camada bronze) |
| **PostgreSQL** | 16 | Banco de metadados do Airflow |
| **Python** | 3.12 | Linguagem principal do pipeline |
| **boto3** | 1.35.0 | Client S3 — mesmo SDK usado no AWS S3 |
| **requests** | 2.32.3 | HTTP client para a NASA API |
| **python-dotenv** | 1.0.1 | Gestão de variáveis de ambiente |
| **pytest** | 8.3.2 | Framework de testes unitários |
| **Docker Compose** | v2 | Orquestração local dos containers |

---

##  Estrutura do Repositório

```
nasa-neows-pipeline/
│
├── .env.example                     # Template de variáveis — commitar
├── .env                             # Valores reais — NUNCA commitar
├── .gitignore                       # .env, data/raw/, __pycache__
├── docker-compose.yml               # Airflow + MinIO + PostgreSQL
├── requirements.txt                 # Dependências Python
├── run_ingestion.py                 # Entrypoint CLI (sem Airflow)
│
├── airflow/
│   ├── start.sh                     # Boot script do container Airflow
│   └── dags/
│       └── nasa_neows_ingestion.py  # DAG — schedule diário
│
├── src/
│   ├── __init__.py
│   └── ingestion/
│       ├── __init__.py
│       ├── config.py                # Variáveis de ambiente e constantes
│       ├── client.py                # NasaClient — GET + retry + backoff
│       ├── storage.py               # MinioStorage — upload + idempotência
│       └── pipeline.py              # Orquestra client + storage
│
└── data/
    └── (ignorado pelo git)
```

---

##  Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — com WSL2 habilitado no Windows
- [Python 3.12+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/)
- Chave de API da NASA — cadastro gratuito em [api.nasa.gov](https://api.nasa.gov)

---

## Configuração do Ambiente

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/nasa-neows-pipeline.git
cd nasa-neows-pipeline
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
```

Abra o `.env` e preencha com seus valores:

```dotenv
# NASA API — gere sua chave em https://api.nasa.gov
NASA_API_KEY=sua_chave_aqui

# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=sua_senha_aqui
AIRFLOW_ADMIN_EMAIL=admin@local.com

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=sua_senha_aqui
```

>  **Importante:** o arquivo `.env` já está no `.gitignore`. Nunca commite credenciais reais. Use sempre o `.env.example` como referência pública.

### 3. Crie o ambiente virtual Python

```bash
python -m venv .venv

# Linux/Mac
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

---

##  Subindo a Infraestrutura

### 1. Suba todos os containers

```bash
docker compose up -d
```

O Docker irá subir em sequência:

1. **PostgreSQL** — banco de metadados do Airflow
2. **MinIO** — object storage com buckets bronze, silver e gold
3. **minio-mc** — cria os buckets automaticamente
4. **Airflow** — executa o `start.sh` que migra o banco, cria o admin e sobe scheduler + webserver

### 2. Acompanhe o boot do Airflow

```bash
docker logs -f airflow
```

Aguarde aparecer:
```
>>> [1/4] Aguardando banco de dados ficar pronto...
>>> [2/4] Inicializando/migrando schema do Airflow...
>>> [3/4] Criando usuário admin...
>>> [4/4] Subindo scheduler em background...
>>> Subindo webserver...
[INFO] Listening at: http://0.0.0.0:8080
```

### 3. Verifique os serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |

### 4. Confirme os buckets criados no MinIO

Acesse http://localhost:9001 e verifique que os buckets `bronze`, `silver` e `gold` foram criados automaticamente pelo `minio-mc`.

---

##  Código de Ingestão

A lógica de ingestão está em `src/ingestion/` e é **completamente independente do Airflow**. Cada módulo tem uma responsabilidade única:

### `config.py` — configuração centralizada

Lê variáveis do `.env` e expõe constantes. É o único arquivo que acessa `os.getenv()`. Todos os outros importam daqui.

```python
from src.ingestion import config

print(config.NASA_BASE_URL)    # https://api.nasa.gov/neo/rest/v1/feed
print(config.BRONZE_BUCKET)    # bronze
print(config.NASA_MAX_WINDOW_DAYS)  # 7
```

### `client.py` — comunicação com a NASA API

Classe `NasaClient` com retry automático e backoff exponencial. Não sabe nada de arquivo ou MinIO.

```python
from src.ingestion.client import NasaClient

client = NasaClient()
payload = client.fetch_feed("2025-01-01", "2025-01-07")
# Retorna dict Python — exatamente o JSON da NASA
```

**Comportamento de retry:**

| Situação | Comportamento |
|----------|---------------|
| Status 200 | Retorna o dict imediatamente |
| Status 429 (rate limit) | Aguarda `2^tentativa` segundos e retenta |
| Status 5xx (servidor) | Aguarda `2^tentativa` segundos e retenta |
| Status 4xx (exceto 429) | Levanta `NasaApiError` imediatamente |
| Timeout | Aguarda `2^tentativa` segundos e retenta |
| Esgotou retries | Levanta exceção tipada |

### `storage.py` — persistência no MinIO

Classe `MinioStorage` com idempotência garantida. Não lê o conteúdo do JSON — trata o payload como caixa preta.

```python
from src.ingestion.storage import MinioStorage

storage = MinioStorage()
result = storage.save(
    data=payload,
    object_key="neows/year=2025/month=01/day=01.json"
)
# result = {"status": "saved", "size_bytes": 142831, "saved_at": "..."}
# Se o arquivo já existir:
# result = {"status": "skipped", "size_bytes": 0, "saved_at": None}
```

### `pipeline.py` — orquestração

Função `run()` que quebra o intervalo em janelas de 7 dias e processa cada uma. Aceita `client` e `storage` como parâmetros opcionais para facilitar testes com mocks.

```python
from src.ingestion.pipeline import run

summary = run(start_date="2025-01-01", end_date="2025-01-31")

print(summary)
# {
#   "total_windows": 5,
#   "saved": 4,
#   "skipped": 1,   ← arquivo já existia, idempotência funcionou
#   "errors": 0,
#   "results": [...]
# }
```

---

##  Executando os Testes

Os testes rodam **sem Docker, sem NASA API, sem MinIO** — tudo mockado com `unittest.mock`.

```bash
# Rodar todos os testes
pytest tests/ -v

# Com relatório de cobertura
pytest tests/ -v --cov=src/ingestion --cov-report=term-missing
```

### Resultado esperado

```
tests/ingestion/test_client.py::TestFetchFeedSuccess::test_retorna_dict_quando_status_200 PASSED
tests/ingestion/test_client.py::TestFetchFeedRateLimit::test_retry_em_429_e_sucede_na_segunda PASSED
tests/ingestion/test_client.py::TestFetchFeedRateLimit::test_levanta_NasaRateLimitError_apos_todos_retries PASSED
tests/ingestion/test_storage.py::TestSaveIdempotency::test_nao_chama_upload_quando_arquivo_existe PASSED
tests/ingestion/test_pipeline.py::TestGenerateWindows::test_janelas_sao_continuas_sem_gaps PASSED
...

51 passed in 0.36s
```

### O que é testado

| Arquivo | Testes | Cobertura |
|---------|--------|-----------|
| `test_client.py` | 12 | Sucesso, rate limit, erro 5xx, erro 4xx, timeout, retry |
| `test_storage.py` | 13 | Upload novo, idempotência, content-type, erro de permissão |
| `test_pipeline.py` | 26 | Janelas de 7 dias, continuidade, erro parcial, sumário |

---

##  Acionando a DAG

### Via Airflow UI

1. Acesse http://localhost:8080
2. Ative a DAG `nasa_neows_ingestion` (toggle na coluna "Active")
3. Clique em **Trigger DAG ▶**
4. Em "Configuration JSON", passe o intervalo desejado:

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-07"
}
```

### Via linha de comando

```bash
# Entrar no container
docker exec -it airflow bash

# Ativar a DAG (começa pausada por padrão)
airflow dags unpause nasa_neows_ingestion

# Acionar com intervalo específico
airflow dags trigger nasa_neows_ingestion \
  --conf '{"start_date": "2025-01-01", "end_date": "2025-01-07"}'

# Acompanhar o estado
airflow dags list-runs -d nasa_neows_ingestion

# Ver logs da task (substituir <run_id> pelo ID real)
airflow tasks logs nasa_neows_ingestion ingest_neows_to_bronze <run_id>
```

### Via CLI (sem Airflow)

Para testar ou executar manualmente sem depender do Airflow:

```bash
# Ingestão de ontem (padrão)
python run_ingestion.py

# Intervalo específico
python run_ingestion.py --start 2025-01-01 --end 2025-01-31
```

### Schedule automático

A DAG está configurada para rodar **todo dia às 06:00 UTC**, ingerindo os dados do dia anterior. A NASA pode demorar algumas horas para disponibilizar dados do dia atual.

```python
schedule = "0 6 * * *"   # minuto hora * * * (cron)
```

---

##  Validando a Ingestão

### 1. Verificar o arquivo no MinIO

Acesse http://localhost:9001 e navegue até:
```
bronze → neows → year=2025 → month=01 → day=01.json
```

### 2. Inspecionar o conteúdo via CLI do MinIO

```bash
docker exec minio-mc mc ls local/bronze/neows/ --recursive
```

### 3. Verificar idempotência

Acione a DAG duas vezes com o mesmo intervalo e confirme que o arquivo não foi duplicado — o log deve mostrar `"status": "skipped"` na segunda execução.

```bash
# Segunda execução — deve retornar skipped
python run_ingestion.py --start 2025-01-01 --end 2025-01-07
```

Saída esperada:
```json
{
  "total_windows": 1,
  "saved": 0,
  "skipped": 1,
  "errors": 0
}
```

### 4. Estrutura de particionamento no MinIO

```
bronze/
└── neows/
    └── year=2025/
        ├── month=01/
        │   ├── day=01.json   ← 2025-01-01 a 2025-01-07
        │   ├── day=08.json   ← 2025-01-08 a 2025-01-14
        │   ├── day=15.json   ← 2025-01-15 a 2025-01-21
        │   ├── day=22.json   ← 2025-01-22 a 2025-01-28
        │   └── day=29.json   ← 2025-01-29 a 2025-01-31
        └── month=02/
            └── ...
```

O particionamento Hive-style (`year=/month=/day=`) é compatível com Apache Spark, AWS Athena, Trino e qualquer engine analítica moderna — facilitando a leitura por data sem precisar ler todos os arquivos.

---

##  Decisões de Arquitetura

### Por que separar ingestão de transformação?

A ingestão é cega ao conteúdo — ela não lê campos do JSON, não conta asteroides, não valida diâmetros. Isso garante que o dado bruto seja sempre preservado exatamente como a NASA enviou. Se a transformação tiver um bug, o reprocessamento acontece lendo os arquivos bronze já salvos, sem chamar a API novamente.

### Por que MinIO em vez de arquivo local?

O boto3 é compatível com qualquer storage S3-compatible. Trocar MinIO por AWS S3 em produção requer mudar apenas o `endpoint_url` na configuração — o resto do código não muda. Isso evita reescrita quando o projeto for para cloud.

### Por que o DAG é uma casca fina?

Todo o código de lógica está em `src/ingestion/` — módulos Python puros, sem importar nada do Airflow. Isso permite:

- Testar com pytest sem Docker
- Rodar via CLI sem Airflow
- Trocar Airflow por Prefect ou qualquer outro orquestrador sem reescrever a lógica

### Por que janelas de 7 dias?

A NASA NeoWs API aceita no máximo 7 dias por requisição. A função `generate_windows()` quebra automaticamente qualquer intervalo em fatias respeitando esse limite — transparente para quem chama o pipeline.

### Por que idempotência via `head_object`?

Antes de qualquer upload, o `storage.py` faz um `head_object` para verificar se o arquivo já existe. Se existir, retorna `{"status": "skipped"}` sem fazer upload. Isso garante que re-executar o pipeline no mesmo dia — seja por falha, por manutenção ou por teste — não duplica dados.

---

##  Próximos Passos

Este repositório cobre a **camada de ingestão (bronze)**. O roadmap completo do data lake:

### Etapa 2 — Transformação (silver) `[próximo PR]`
- Ler JSON bruto do bucket `bronze`
- Normalizar com pandas: explodir `near_earth_objects` em tabelas `asteroids` e `close_approaches`
- Salvar em Parquet no bucket `silver` com schema definido
- DAG de transformação encadeada após a ingestão

### Etapa 3 — Agregações analíticas (gold)
- Criar tabela `daily_summary` com métricas por dia
- Top asteroides por risco, velocidade média, near-misses por mês
- Salvar em DuckDB para consulta SQL local

### Etapa 4 — Orquestração avançada
- Separar webserver e scheduler em containers distintos
- Adicionar sensores entre DAGs (ingestão → transformação → gold)
- Alertas por e-mail em caso de falha

### Etapa 5 — Dashboard
- Streamlit conectado ao DuckDB
- Gráfico de asteroides por dia, scatter velocidade × distância
- Deploy gratuito no Streamlit Cloud

---

##  Autor

Desenvolvido como projeto de portfólio de engenharia de dados — demonstrando práticas de engenharia de dados aplicadas a pipelines de dados: responsabilidade única, testabilidade, idempotência e portabilidade para cloud.

