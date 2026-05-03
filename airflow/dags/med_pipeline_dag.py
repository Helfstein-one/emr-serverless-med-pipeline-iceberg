"""
============================================================
med_pipeline_dag.py — DAG do Pipeline de Saúde
============================================================
Orquestra o pipeline completo do Lakehouse de Saúde:
1. Ingestão Bronze (CSV → Parquet)
2. Promoção Silver (Parquet → Iceberg)
3. dbt Silver (limpeza + contratos)
4. dbt Test (validação de contratos)
5. dbt Gold (modelagem dimensional)
6. dbt Docs (documentação)

Os jobs Spark são executados via BashOperator
chamando spark-submit dentro do container Spark.
Os comandos dbt rodam diretamente no container Airflow.
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# Configurações padrão do DAG
# ============================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}

# Caminho do projeto dbt dentro do container Airflow
DBT_PROJECT_DIR = "/app/dbt_project"
DBT_PROFILES_DIR = "/app/dbt_project"

# Comando base do dbt
DBT_BASE = (
    f"dbt {{cmd}} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
)

# Comando para executar spark-submit no container Spark via docker/podman exec
# NOTA: Em produção, usar SparkSubmitOperator com conexão configurada.
# Para dev local, os jobs rodam via spark-submit no container Spark.
SPARK_SUBMIT = "spark-submit --master local[*] /app/spark/jobs/{script}"


# ============================================================
# Definição do DAG
# ============================================================
with DAG(
    dag_id="med_pipeline",
    description="Pipeline completo do Lakehouse de Saúde — Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["saude", "lakehouse", "iceberg", "medallion"],
    doc_md="""
    ## 🏥 Pipeline Médico — Lakehouse de Saúde

    Pipeline de dados completo para internações hospitalares.

    ### Camadas:
    - **Bronze**: Ingestão de CSV → Parquet (MinIO)
    - **Silver**: Limpeza + Contratos de Dados (Iceberg via Nessie)
    - **Gold**: Modelagem Dimensional — Star Schema (Iceberg)

    ### Fluxo:
    `ingest_bronze → promote_silver → dbt_silver → dbt_test → dbt_gold → dbt_docs`
    """,
) as dag:
    # ========================================================
    # TASK 1: Ingestão Bronze (CSV → Parquet)
    # ========================================================
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=SPARK_SUBMIT.format(script="ingest_bronze.py"),
        doc_md="Lê CSVs do bucket bronze/raw/ e grava como Parquet em bronze/parquet/",
    )

    # ========================================================
    # TASK 2: Promoção Silver (Parquet → Iceberg)
    # ========================================================
    promote_silver = BashOperator(
        task_id="promote_silver",
        bash_command=SPARK_SUBMIT.format(script="promote_silver.py"),
        doc_md="Lê Parquet do Bronze e insere/atualiza tabela Iceberg via MERGE INTO",
    )

    # ========================================================
    # TASK 3: dbt Silver (limpeza + padronização)
    # ========================================================
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=DBT_BASE.format(cmd="run --select silver"),
    )

    # ========================================================
    # TASK 4: dbt Test (validação de contratos)
    # ========================================================
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_BASE.format(cmd="test"),
        doc_md="⚠️ Se esse teste falhar, o pipeline é interrompido. Contratos de dados violados.",
    )

    # ========================================================
    # TASK 5: dbt Gold (modelagem dimensional)
    # ========================================================
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=DBT_BASE.format(cmd="run --select gold"),
    )

    # ========================================================
    # TASK 6: dbt Docs (geração de documentação)
    # ========================================================
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=DBT_BASE.format(cmd="docs generate"),
    )

    # ========================================================
    # DEPENDÊNCIAS
    # ========================================================
    (
        ingest_bronze
        >> promote_silver
        >> dbt_run_silver
        >> dbt_test
        >> dbt_run_gold
        >> dbt_docs_generate
    )
