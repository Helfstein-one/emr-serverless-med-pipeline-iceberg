import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)
from spark.jobs.promote_silver import (
    create_namespace_if_not_exists, 
    create_iceberg_table_if_not_exists, 
    FULL_TABLE
)

def test_silver_table_schema_contract(spark):
    """
    Testa se o schema da tabela Silver Iceberg está de acordo com o contrato de dados
    esperado para o catálogo Nessie.
    """
    # 1. Garante a criação da tabela no catálogo (via spark session local do teste)
    create_namespace_if_not_exists(spark)
    create_iceberg_table_if_not_exists(spark)
    
    # 2. Recupera o schema atual da tabela do catálogo Nessie/Iceberg local
    df = spark.table(FULL_TABLE)
    actual_schema = df.schema
    
    # 3. Define o contrato de dados esperado
    expected_schema = StructType([
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
    
    # 4. Compara ambos os schemas (Nomes e Tipos de Dados)
    assert len(actual_schema.fields) == len(expected_schema.fields), "O número de colunas no schema difere do contrato."
    
    for expected_field in expected_schema.fields:
        assert expected_field.name in actual_schema.names, f"A coluna esperada '{expected_field.name}' não está no schema."
        actual_field = actual_schema[expected_field.name]
        assert actual_field.dataType == expected_field.dataType, f"O tipo da coluna '{expected_field.name}' difere: esperado {expected_field.dataType}, atual {actual_field.dataType}."
