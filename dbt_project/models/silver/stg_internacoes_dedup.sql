{{
    config(
        materialized='incremental',
        file_format='iceberg',
        incremental_strategy='merge',
        unique_key='id_internacao',
        partition_by=[{'field': 'data_internacao', 'data_type': 'date', 'granularity': 'month'}]
    )
}}

{#
============================================================
stg_internacoes_dedup.sql — Deduplicação Silver
============================================================
Remove registros duplicados utilizando window function
ROW_NUMBER() particionado por id_internacao.
Mantém o registro com o ingestion_at mais recente.
============================================================
#}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_internacao
            ORDER BY ingestion_at DESC, dbt_processed_at DESC
        ) AS row_num
    FROM {{ ref('stg_internacoes') }}
)

SELECT
    id_internacao,
    data_internacao,
    data_alta,
    cid_principal,
    cid_secundario,
    nome_hospital,
    uf,
    municipio,
    cod_municipio_ibge,
    especialidade,
    tipo_atendimento,
    carater_atendimento,
    valor_total,
    dias_permanencia,
    sexo,
    idade,
    ingestion_at,
    source_file,
    dbt_processed_at
FROM ranked
WHERE row_num = 1
