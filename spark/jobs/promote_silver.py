"""
============================================================
promote_silver.py — Promoção para Silver (Iceberg via Nessie)
============================================================
Lê dados Parquet do bucket Bronze e grava como tabela Iceberg
no catálogo Nessie (local.silver.internacoes).

Particionamento: months(data_internacao)
Estratégia: MERGE INTO para idempotência (upsert por id_internacao).

Uso:
    spark-submit /app/spark/jobs/promote_silver.py
============================================================
"""

import sys
import os

from pyspark.sql import functions as F

# Adiciona o diretório raiz ao path para importar config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.config import create_spark_session


# ============================================================
# Constantes
# ============================================================
NAMESPACE = "silver"
TABLE_NAME = "internacoes"
FULL_TABLE = f"local.{NAMESPACE}.{TABLE_NAME}"

BRONZE_PATH = "s3a://bronze/parquet/internacoes/"


def create_namespace_if_not_exists(spark) -> None:
    """Cria o namespace 'silver' no catálogo Nessie se não existir."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{NAMESPACE}")
    print(f"📁 Namespace 'local.{NAMESPACE}' garantido.")


def create_iceberg_table_if_not_exists(spark) -> None:
    """
    Cria a tabela Iceberg com particionamento por months(data_internacao)
    caso ela ainda não exista.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            id_internacao       STRING      COMMENT 'Identificador único da internação',
            data_internacao     DATE        COMMENT 'Data de início da internação',
            data_alta           DATE        COMMENT 'Data da alta hospitalar',
            cid_principal       STRING      COMMENT 'Código CID-10 principal',
            cid_secundario      STRING      COMMENT 'Código CID-10 secundário',
            nome_hospital       STRING      COMMENT 'Nome do hospital',
            uf                  STRING      COMMENT 'Unidade Federativa',
            municipio           STRING      COMMENT 'Nome do município',
            cod_municipio_ibge  STRING      COMMENT 'Código IBGE do município',
            especialidade       STRING      COMMENT 'Especialidade médica',
            tipo_atendimento    STRING      COMMENT 'Tipo de atendimento',
            valor_total         DOUBLE      COMMENT 'Valor total da internação (R$)',
            dias_permanencia    INT         COMMENT 'Dias de permanência',
            sexo                STRING      COMMENT 'Sexo do paciente (M/F)',
            idade               INT         COMMENT 'Idade do paciente',
            carater_atendimento STRING      COMMENT 'Caráter do atendimento',
            ingestion_at        STRING      COMMENT 'Timestamp de ingestão',
            source_file         STRING      COMMENT 'Arquivo fonte'
        )
        USING iceberg
        PARTITIONED BY (months(data_internacao))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'commit.retry.num-retries' = '3',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '5'
        )
    """)
    print(
        f"📋 Tabela '{FULL_TABLE}' garantida com particionamento months(data_internacao)."
    )


def promote_to_silver() -> int:
    """
    Lê dados do Bronze e faz MERGE INTO na tabela Silver Iceberg.

    Retorna:
        Número de registros processados.
    """
    spark = create_spark_session("PromoteSilver")

    # Garante namespace e tabela
    create_namespace_if_not_exists(spark)
    create_iceberg_table_if_not_exists(spark)

    # Lê Parquet do Bronze
    print(f"📥 Lendo Parquet de: {BRONZE_PATH}")
    df_bronze = spark.read.parquet(BRONZE_PATH)

    record_count = df_bronze.count()
    print(f"📊 Registros do Bronze: {record_count}")

    if record_count == 0:
        print("⚠️  Nenhum registro encontrado no Bronze. Encerrando.")
        spark.stop()
        return 0

    # ---- Limpeza básica para Silver ----
    df_clean = (
        df_bronze
        # Padroniza CIDs para maiúsculas e remove espaços
        .withColumn("cid_principal", F.upper(F.trim(F.col("cid_principal"))))
        .withColumn("cid_secundario", F.upper(F.trim(F.col("cid_secundario"))))
        # Padroniza nomes de hospitais (Title Case)
        .withColumn("nome_hospital", F.initcap(F.trim(F.col("nome_hospital"))))
        # Padroniza sexo
        .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
        # Filtra idades inválidas
        .withColumn(
            "idade", F.when(F.col("idade") >= 0, F.col("idade")).otherwise(F.lit(None))
        )
        # Remove nulos no campo sexo (substitui por 'NI' = Não Informado)
        .withColumn(
            "sexo",
            F.when(
                (F.col("sexo").isNull()) | (F.col("sexo") == ""), F.lit("NI")
            ).otherwise(F.col("sexo")),
        )
    )

    # Registra como view temporária para o MERGE INTO
    df_clean.createOrReplaceTempView("bronze_staging")

    # ---- MERGE INTO (Upsert idempotente) ----
    print(f"🔄 Executando MERGE INTO {FULL_TABLE}...")

    spark.sql(f"""
        MERGE INTO {FULL_TABLE} AS target
        USING bronze_staging AS source
        ON target.id_internacao = source.id_internacao
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Contagem final
    final_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE}").collect()[0][
        "cnt"
    ]
    print("✅ Promoção Silver concluída!")
    print(f"   ├── Registros processados: {record_count}")
    print(f"   └── Total na tabela Silver: {final_count}")

    spark.stop()
    return record_count


if __name__ == "__main__":
    promote_to_silver()
