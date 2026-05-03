import os
import pytest
from unittest.mock import patch
from spark.jobs.ingest_bronze import ingest_bronze, SCHEMA_INTERNACOES


def test_ingest_bronze_integration(spark, test_data_dir):
    """
    Testa a ingestão de um CSV cru (Bronze) e gravação em Parquet,
    verificando as transformações aplicadas (ingestion_at, source_file e particionamento).
    """
    # 1. Preparar os dados de origem (CSV mockado)
    source_dir = os.path.join(test_data_dir, "raw_bronze")
    os.makedirs(source_dir, exist_ok=True)
    csv_file = os.path.join(source_dir, "internacoes_teste.csv")
    
    csv_content = (
        "id_internacao,data_internacao,data_alta,cid_principal,cid_secundario,"
        "nome_hospital,uf,municipio,cod_municipio_ibge,especialidade,"
        "tipo_atendimento,valor_total,dias_permanencia,sexo,idade,carater_atendimento\n"
        "INT-001,2023-01-01,2023-01-05,J00,,HOSPITAL TESTE,SP,SAO PAULO,3550308,"
        "CLINICA MEDICA,URGENCIA,1500.00,4,M,45,URGENCIA\n"
        "INT-002,2023-01-02,,A09,,HOSPITAL GERAL,RJ,RIO DE JANEIRO,3304557,"
        "PEDIATRIA,URGENCIA,500.50,1,F,8,URGENCIA\n"
    )
    with open(csv_file, "w") as f:
        f.write(csv_content)

    target_dir = os.path.join(test_data_dir, "parquet_bronze")

    # 2. Executar o job, mockando a sessão do Spark para usar a local
    with patch("spark.jobs.ingest_bronze.create_spark_session", return_value=spark):
        records_processed = ingest_bronze(source_path=source_dir, target_path=target_dir)

    # 3. Asserções (Verificações)
    assert records_processed == 2

    # Verifica se os arquivos parquet foram gravados e lê de volta
    df_result = spark.read.parquet(target_dir)
    
    # Confere a quantidade total de registros lidos do Parquet
    assert df_result.count() == 2

    # Confere se os metadados de carga foram adicionados
    columns = df_result.columns
    assert "ingestion_at" in columns
    assert "source_file" in columns

    # Confere tipos e valores
    data = df_result.orderBy("id_internacao").collect()
    
    # Linha 1
    assert data[0]["id_internacao"] == "INT-001"
    assert data[0]["uf"] == "SP"
    assert data[0]["valor_total"] == 1500.00
    assert data[0]["dias_permanencia"] == 4
    
    # Linha 2
    assert data[1]["id_internacao"] == "INT-002"
    assert data[1]["uf"] == "RJ"
    assert data[1]["idade"] == 8

    # O source_file deve conter o caminho do csv
    assert "internacoes_teste.csv" in data[0]["source_file"]
