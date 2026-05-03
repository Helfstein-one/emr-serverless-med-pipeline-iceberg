# ============================================================
# Airflow 2.9 + dbt-spark
# Imagem estendida para orquestração do pipeline
# ============================================================
# NOTA: Em vez de instalar Spark binaries dentro do Airflow,
# usamos BashOperator com podman exec para executar
# spark-submit no container Spark.
# ============================================================
FROM apache/airflow:2.9.1

USER root

# --- Instala dependências de sistema ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- Cria diretórios do projeto ---
RUN mkdir -p /app/spark/jobs /app/spark/utils /app/dbt_project /app/data

USER airflow

# --- Instala dependências Python ---
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# --- Variáveis de ambiente do Airflow ---
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
