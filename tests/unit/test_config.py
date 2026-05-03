import os
import pytest
from pyspark.sql import SparkSession
from spark.jobs.config import create_spark_session


from unittest.mock import patch

@patch("spark.jobs.config.SparkSession")
def test_create_spark_session(mock_spark_session, monkeypatch):
    """
    Testa se a função de criar SparkSession retorna o objeto correto
    e se as variáveis de ambiente alteram seu comportamento.
    """
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testkey")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testsecret")

    # Mock the builder pattern
    mock_builder = mock_spark_session.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = "mocked_spark_session"

    spark = create_spark_session(app_name="UnitTestSession")
    
    assert spark == "mocked_spark_session"
    mock_builder.appName.assert_called_with("UnitTestSession")
    mock_builder.master.assert_called_with("local[1]")
    
    # Check if specific configs were set
    mock_builder.config.assert_any_call("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    mock_builder.config.assert_any_call("spark.hadoop.fs.s3a.access.key", "testkey")
    mock_builder.config.assert_any_call("spark.hadoop.fs.s3a.secret.key", "testsecret")
