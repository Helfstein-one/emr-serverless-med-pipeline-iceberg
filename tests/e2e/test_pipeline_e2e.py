import os
import pytest
from unittest.mock import patch
from spark.jobs.ingest_bronze import ingest_bronze
from spark.jobs.promote_silver import promote_to_silver, FULL_TABLE

def test_end_to_end_pipeline(spark, test_data_dir):
    """
    Testa o fluxo completo End-to-End:
    1. Lê da camada Bronze/Raw (CSV)
    2. Ingere para Bronze/Parquet
    3. Promove para Silver (Iceberg via MERGE)
    """
    # 1. Setup de diretórios mockados
    raw_dir = os.path.join(test_data_dir, "e2e_raw")
    parquet_dir = os.path.join(test_data_dir, "e2e_parquet")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Criar arquivo CSV
    csv_file = os.path.join(raw_dir, "pacientes.csv")
    csv_content = (
        "id_internacao,data_internacao,data_alta,cid_principal,cid_secundario,"
        "nome_hospital,uf,municipio,cod_municipio_ibge,especialidade,"
        "tipo_atendimento,valor_total,dias_permanencia,sexo,idade,carater_atendimento\n"
        "INT-E2E-1,2023-05-01,2023-05-10, j00 ,, hospital central ,MG,BELO HORIZONTE,3106200,"
        "CLINICA,URGENCIA,2500.00,9, m ,-2,URGENCIA\n"
    )
    with open(csv_file, "w") as f:
        f.write(csv_content)

    # 2. Executar Ingestão Bronze (Mockando as dependências externas e S3)
    with patch("spark.jobs.ingest_bronze.create_spark_session", return_value=spark):
        ingest_bronze(source_path=raw_dir, target_path=parquet_dir)

    # 3. Executar Promoção Silver (Mockando as dependências externas e o path do Parquet)
    with patch("spark.jobs.promote_silver.create_spark_session", return_value=spark), \
         patch("spark.jobs.promote_silver.BRONZE_PATH", parquet_dir):
        promote_to_silver()

    # 4. Validar resultado E2E
    df_silver = spark.sql(f"SELECT * FROM {FULL_TABLE} WHERE id_internacao = 'INT-E2E-1'")
    assert df_silver.count() == 1
    
    data = df_silver.collect()[0]
    
    # Verifica o pipeline completo: leitura, adição de metadados, limpeza de strings e tipos
    assert data["uf"] == "MG"
    assert data["nome_hospital"] == "Hospital Central" # Formatação na Silver
    assert data["cid_principal"] == "J00" # Limpeza na Silver
    assert data["sexo"] == "M" # Limpeza na Silver
    assert data["idade"] is None # Limpeza na Silver (idade negativa nula)
    assert "ingestion_at" in data # Adicionado na Ingestão Bronze
    assert "pacientes.csv" in data["source_file"] # Adicionado na Ingestão Bronze
