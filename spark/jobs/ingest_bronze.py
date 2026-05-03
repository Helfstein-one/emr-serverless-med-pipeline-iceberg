"""
============================================================
ingest_bronze.py — Ingestão Bronze (Raw → Parquet)
============================================================
Lê arquivos CSV do bucket bronze/raw/ no MinIO,
adiciona metadados de carga (ingestion_at, source_file)
e grava como Parquet no bucket bronze/parquet/.

O processo é IDEMPOTENTE: utiliza modo overwrite
no destino particionado.

Uso:
    spark-submit /app/spark/jobs/ingest_bronze.py
============================================================
"""

import sys
import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
)

# Adiciona o diretório raiz ao path para importar config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.config import create_spark_session


# ============================================================
# Schema explícito do CSV de internações
# ============================================================
SCHEMA_INTERNACOES = StructType(
    [
        StructField("id_internacao", StringType(), True),
        StructField("data_internacao", DateType(), True),
        StructField("data_alta", DateType(), True),
        StructField("cid_principal", StringType(), True),
        StructField("cid_secundario", StringType(), True),
        StructField("nome_hospital", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("cod_municipio_ibge", StringType(), True),
        StructField("especialidade", StringType(), True),
        StructField("tipo_atendimento", StringType(), True),
        StructField("valor_total", DoubleType(), True),
        StructField("dias_permanencia", IntegerType(), True),
        StructField("sexo", StringType(), True),
        StructField("idade", IntegerType(), True),
        StructField("carater_atendimento", StringType(), True),
    ]
)


def ingest_bronze(
    source_path: str = "s3a://bronze/raw/internacoes/",
    target_path: str = "s3a://bronze/parquet/internacoes/",
) -> int:
    """
    Ingere dados CSV do bucket bronze e grava como Parquet.

    Parâmetros:
        source_path: Caminho S3A do CSV fonte.
        target_path: Caminho S3A para gravação do Parquet.

    Retorna:
        Número de registros processados.
    """
    spark = create_spark_session("IngestBronze")

    print(f"📥 Lendo CSVs de: {source_path}")

    # Lê CSV com schema explícito
    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("dateFormat", "yyyy-MM-dd")
        .option("encoding", "UTF-8")
        .schema(SCHEMA_INTERNACOES)
        .csv(source_path)
    )

    record_count = df_raw.count()
    print(f"📊 Registros lidos: {record_count}")

    # Adiciona metadados de carga
    df_enriched = df_raw.withColumn(
        "ingestion_at", F.lit(datetime.utcnow().isoformat())
    ).withColumn("source_file", F.input_file_name())

    # Grava como Parquet (overwrite para idempotência)
    print(f"💾 Gravando Parquet em: {target_path}")
    (df_enriched.write.mode("overwrite").partitionBy("uf").parquet(target_path))

    print(f"✅ Ingestão Bronze concluída: {record_count} registros processados.")

    spark.stop()
    return record_count


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "s3a://bronze/raw/internacoes/"
    target = sys.argv[2] if len(sys.argv) > 2 else "s3a://bronze/parquet/internacoes/"
    ingest_bronze(source, target)
