"""
============================================================
config.py — Fábrica de SparkSession
============================================================
Cria uma SparkSession configurada para Iceberg + Nessie + S3A.
Todas as configurações são lidas de variáveis de ambiente,
permitindo trocar de MinIO local → AWS S3 sem modificar código.

Uso:
    from spark.jobs.config import create_spark_session
    spark = create_spark_session("MeuJob")
============================================================
"""

import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "MedPipeline") -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para o Lakehouse.

    Parâmetros:
        app_name: Nome da aplicação Spark.

    Retorna:
        SparkSession configurada com Iceberg, Nessie e S3A.
    """

    # ---- Variáveis de ambiente (com defaults para MinIO local) ----
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
    nessie_ref = os.getenv("NESSIE_REF", "main")
    warehouse = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")
    spark_master = os.getenv("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        # --- Extensões SQL ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # --- Catálogo Iceberg via Nessie ---
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.local.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.local.uri", nessie_uri)
        .config("spark.sql.catalog.local.ref", nessie_ref)
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        # --- Hadoop S3A ---
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # --- Catálogo padrão ---
        .config("spark.sql.defaultCatalog", "local")
        # --- Performance ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    return builder.getOrCreate()
