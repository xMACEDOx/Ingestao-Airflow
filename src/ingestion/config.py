"""
config.py
─────────
Fonte única de configuração do projeto.
Todos os outros módulos importam daqui — nunca hardcodam valores.

Variáveis lidas do .env (via python-dotenv):
    NASA_API_KEY      chave da API da NASA
    MINIO_ENDPOINT    ex: http://localhost:9000
    MINIO_ACCESS_KEY  usuário do MinIO
    MINIO_SECRET_KEY  senha do MinIO
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Sobe até a raiz do projeto para encontrar o .env
_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")


def _require(var: str) -> str:
    """Lê variável de ambiente. Levanta erro claro se estiver ausente."""
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Variável de ambiente obrigatória não definida: '{var}'\n"
            f"Copie .env.example para .env e preencha o valor."
        )
    return value


# ── NASA API ──────────────────────────────────────────────────
NASA_API_KEY  = _require("NASA_API_KEY")
NASA_BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"

# Limite da API: máximo de 7 dias por requisição
NASA_MAX_WINDOW_DAYS = 7

# Timeout em segundos para cada requisição HTTP
NASA_REQUEST_TIMEOUT = 30

# Retry: quantas tentativas em caso de erro 429 ou 5xx
NASA_MAX_RETRIES = 3

# Backoff inicial em segundos (dobra a cada tentativa)
NASA_BACKOFF_BASE = 2


# ── MinIO / S3 ────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = _require("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = _require("MINIO_SECRET_KEY")

# Bucket onde os JSONs brutos serão salvos
BRONZE_BUCKET = "bronze"

# Prefixo dentro do bucket — facilita busca e particionamento
# Caminho final: bronze/neows/year=2024/month=01/day=15.json
BRONZE_PREFIX = "neows"
