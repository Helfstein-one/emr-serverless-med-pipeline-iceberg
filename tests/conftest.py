import pytest
import os
import tempfile
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Cria uma SparkSession local para os testes, 
    sem necessidade de conectar ao Minio/S3.
    """
    spark = (
        SparkSession.builder.appName("TestMedPipeline")
        .master("local[2]")
        # Configurações minimalistas para o teste local não quebrar com S3
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
        # Para evitar problemas com Iceberg não configurado nos testes unitários mais simples
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", tempfile.mkdtemp())
        .getOrCreate()
    )
    
    # Previne que os scripts chamem spark.stop() e quebrem a sessão de teste
    from unittest import mock
    with mock.patch.object(spark, 'stop'):
        yield spark
    
    spark._jsc.sc().stop() # Para o SparkContext diretamente no teardown


@pytest.fixture(scope="session")
def test_data_dir():
    """Retorna um diretório temporário para ser usado como base de dados nos testes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
