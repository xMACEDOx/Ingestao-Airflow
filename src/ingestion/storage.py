"""
storage.py
──────────
Responsabilidade única: persistir o JSON bruto no MinIO (camada bronze).

O que faz:
    - Recebe um dict Python e um nome de objeto (chave S3)
    - Verifica se o objeto já existe no bucket (idempotência)
    - Serializa para JSON e faz upload via boto3
    - Retorna metadados da operação (salvo, pulado, tamanho...)

O que NÃO faz:
    - Não lê nem interpreta o conteúdo do dict
    - Não sabe nada sobre a NASA ou datas
    - Não cria transformações — só salva bytes

Uso standalone (para testar sem Airflow):
    from src.ingestion.storage import MinioStorage
    storage = MinioStorage()
    result = storage.save(data=payload, object_key="neows/year=2024/month=01/day=01.json")
"""

import json
import logging
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

from src.ingestion import config

logger = logging.getLogger(__name__)


class MinioStorage:
    """
    Armazena payloads JSON no bucket bronze do MinIO.

    O cliente boto3 é compatível com qualquer storage S3-compatible.
    Trocar MinIO por AWS S3 em produção = mudar só o endpoint nas configs.

    Exemplo:
        storage = MinioStorage()
        result = storage.save(
            data={"element_count": 142, ...},
            object_key="neows/year=2024/month=01/day=01.json"
        )
        # result = {"status": "saved", "object_key": "...", "size_bytes": 142831}
    """

    def __init__(
        self,
        endpoint:   str = None,
        access_key: str = None,
        secret_key: str = None,
        bucket:     str = None,
    ):
        self.bucket = bucket or config.BRONZE_BUCKET

        self.client = boto3.client(
            "s3",
            endpoint_url          = endpoint   or config.MINIO_ENDPOINT,
            aws_access_key_id     = access_key or config.MINIO_ACCESS_KEY,
            aws_secret_access_key = secret_key or config.MINIO_SECRET_KEY,
            # Necessário para MinIO — desabilita verificação de região AWS
            region_name           = "us-east-1",
            config                = boto3.session.Config(signature_version="s3v4"),
        )

    # ─────────────────────────────────────────────────────────────
    #  API pública
    # ─────────────────────────────────────────────────────────────

    def save(self, data: dict, object_key: str) -> dict:
        """
        Salva um dict como JSON no bucket bronze.

        Idempotente: se o objeto já existir, retorna status "skipped"
        sem fazer upload novamente.

        Args:
            data:       dict Python a ser serializado como JSON
            object_key: caminho dentro do bucket
                        ex: "neows/year=2024/month=01/day=01.json"

        Returns:
            dict com campos:
                status      "saved" | "skipped"
                object_key  caminho completo no bucket
                bucket      nome do bucket
                size_bytes  tamanho do arquivo (0 se skipped)
                saved_at    timestamp UTC (None se skipped)
        """
        if self._object_exists(object_key):
            logger.info(
                "Objeto já existe, pulando upload | bucket=%s key=%s",
                self.bucket, object_key
            )
            return {
                "status":     "skipped",
                "object_key": object_key,
                "bucket":     self.bucket,
                "size_bytes": 0,
                "saved_at":   None,
            }

        return self._upload(data, object_key)

    def object_exists(self, object_key: str) -> bool:
        """Verifica publicamente se um objeto existe (útil para testes)."""
        return self._object_exists(object_key)

    # ─────────────────────────────────────────────────────────────
    #  Métodos internos
    # ─────────────────────────────────────────────────────────────

    def _object_exists(self, object_key: str) -> bool:
        """
        Verifica existência via head_object.
        Retorna False para qualquer erro que não seja 404.
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return False
            # Erro inesperado (permissão, rede etc.) — propaga
            logger.error(
                "Erro ao verificar existência do objeto | key=%s | erro=%s",
                object_key, str(e)
            )
            raise

    def _upload(self, data: dict, object_key: str) -> dict:
        """
        Serializa o dict para JSON e faz upload para o MinIO.
        indent=2 para que os arquivos sejam legíveis por humanos.
        """
        raw_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        buffer    = BytesIO(raw_bytes)
        size      = len(raw_bytes)

        logger.info(
            "Fazendo upload | bucket=%s key=%s | %d bytes",
            self.bucket, object_key, size
        )

        self.client.upload_fileobj(
            Fileobj     = buffer,
            Bucket      = self.bucket,
            Key         = object_key,
            ExtraArgs   = {"ContentType": "application/json"},
        )

        saved_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Upload concluído | bucket=%s key=%s | saved_at=%s",
            self.bucket, object_key, saved_at
        )

        return {
            "status":     "saved",
            "object_key": object_key,
            "bucket":     self.bucket,
            "size_bytes": size,
            "saved_at":   saved_at,
        }
