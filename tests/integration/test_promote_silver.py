import os
import pytest
from unittest.mock import patch
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)
from datetime import date
from spark.jobs.promote_silver import promote_to_silver, FULL_TABLE


def test_promote_silver_integration(spark, test_data_dir):
    """
    Testa a promoção dos dados da camada Bronze para a Silver (Iceberg),
    verificando a limpeza de dados e o MERGE INTO.
    """
    # 1. Preparar os dados de origem (Mockando Parquet no diretório "bronze")
    bronze_dir = os.path.join(test_data_dir, "mock_bronze_parquet")
    os.makedirs(bronze_dir, exist_ok=True)
    
    # Criar DataFrame fictício com sujeiras que o script limpa (espaços, minúsculas, nulls)
    schema = StructType([
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
        StructField("ingestion_at", StringType(), True),
        StructField("source_file", StringType(), True)
    ])
    
    data = [
        ("INT-001", date(2023, 1, 1), date(2023, 1, 5), " j00 ", None, "hospital teste  ", "SP", "SAO PAULO", "3550308", "CLINICA", "URGENCIA", 1500.0, 4, " m ", -5, "URGENCIA", "2023", "f.csv"),
        ("INT-002", date(2023, 1, 2), None, "a09", " b01 ", "hospital geral", "RJ", "RIO", "3304557", "PEDIATRIA", "URGENCIA", 500.0, 1, None, 8, "URGENCIA", "2023", "f.csv"),
    ]
    
    df_bronze = spark.createDataFrame(data, schema)
    df_bronze.write.mode("overwrite").parquet(bronze_dir)

    # 2. Executar o job, mockando a sessão do Spark e o caminho do Bronze
    with patch("spark.jobs.promote_silver.create_spark_session", return_value=spark), \
         patch("spark.jobs.promote_silver.BRONZE_PATH", bronze_dir):
        
        records_processed = promote_to_silver()

    # 3. Asserções (Verificações)
    assert records_processed == 2

    # Verifica se os dados foram inseridos na tabela Silver Iceberg e limpos corretamente
    df_silver = spark.sql(f"SELECT * FROM {FULL_TABLE} WHERE id_internacao IN ('INT-001', 'INT-002')")
    assert df_silver.count() == 2

    silver_data = df_silver.orderBy("id_internacao").collect()
    
    # Linha 1 (Verifica limpeza)
    # cid_principal -> "J00" (upper e trim)
    assert silver_data[0]["cid_principal"] == "J00"
    # nome_hospital -> "Hospital Teste" (initcap e trim)
    assert silver_data[0]["nome_hospital"] == "Hospital Teste"
    # sexo -> "M" (upper e trim)
    assert silver_data[0]["sexo"] == "M"
    # idade -> None (idade < 0 filtrada)
    assert silver_data[0]["idade"] is None
    
    # Linha 2 (Verifica limpeza nulos)
    # cid_secundario -> "B01"
    assert silver_data[1]["cid_secundario"] == "B01"
    # sexo -> "NI" (quando null ou vazio)
    assert silver_data[1]["sexo"] == "NI"
    assert silver_data[1]["idade"] == 8
