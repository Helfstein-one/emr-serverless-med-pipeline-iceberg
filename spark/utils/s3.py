"""
============================================================
s3.py — Utilitários de comunicação S3A / MinIO
============================================================
Funções auxiliares para operações com o MinIO/S3
usando boto3. Lê credenciais de variáveis de ambiente.
============================================================
"""

import os
import boto3
from botocore.client import Config


def get_s3_client():
    """
    Cria e retorna um cliente boto3 para S3/MinIO.
    Configurado via variáveis de ambiente.

    Retorna:
        boto3.client: Cliente S3 configurado.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def upload_file(local_path: str, bucket: str, key: str) -> None:
    """
    Envia um arquivo local para o bucket S3/MinIO.

    Parâmetros:
        local_path: Caminho do arquivo local.
        bucket: Nome do bucket de destino.
        key: Chave (path) do objeto no bucket.
    """
    client = get_s3_client()
    client.upload_file(local_path, bucket, key)
    print(f"✅ Upload: {local_path} → s3://{bucket}/{key}")


def list_objects(bucket: str, prefix: str = "") -> list:
    """
    Lista objetos em um bucket S3/MinIO.

    Parâmetros:
        bucket: Nome do bucket.
        prefix: Prefixo para filtrar objetos.

    Retorna:
        Lista de chaves dos objetos encontrados.
    """
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    return [obj["Key"] for obj in objects]


def check_bucket_exists(bucket: str) -> bool:
    """
    Verifica se um bucket existe no S3/MinIO.

    Parâmetros:
        bucket: Nome do bucket.

    Retorna:
        True se o bucket existir, False caso contrário.
    """
    client = get_s3_client()
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except client.exceptions.ClientError:
        return False


def create_bucket_if_not_exists(bucket: str) -> None:
    """
    Cria um bucket se ele não existir.

    Parâmetros:
        bucket: Nome do bucket a ser criado.
    """
    if not check_bucket_exists(bucket):
        client = get_s3_client()
        client.create_bucket(Bucket=bucket)
        print(f"✅ Bucket '{bucket}' criado.")
    else:
        print(f"ℹ️  Bucket '{bucket}' já existe.")
