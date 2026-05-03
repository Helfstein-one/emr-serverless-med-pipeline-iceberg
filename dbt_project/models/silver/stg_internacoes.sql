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
stg_internacoes.sql — Staging Silver
============================================================
Limpeza e padronização dos dados da camada Silver (Iceberg).
- Padroniza CIDs para UPPER CASE
- Normaliza nomes de hospitais (Title Case)
- Substitui sexo nulo/vazio por 'NI'
- Filtra idades inválidas (negativas → NULL)
- Adiciona timestamp de processamento dbt
============================================================
#}

WITH source AS (
    SELECT *
    FROM {{ source('silver_raw', 'internacoes') }}
    {% if is_incremental() %}
    WHERE ingestion_at > (
        SELECT COALESCE(MAX(ingestion_at), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
),

cleaned AS (
    SELECT
        -- Identificador
        TRIM(id_internacao)                                    AS id_internacao,

        -- Datas
        CAST(data_internacao AS DATE)                          AS data_internacao,
        CAST(data_alta AS DATE)                                AS data_alta,

        -- CIDs padronizados (UPPER + TRIM)
        UPPER(TRIM(cid_principal))                             AS cid_principal,
        UPPER(TRIM(COALESCE(cid_secundario, '')))              AS cid_secundario,

        -- Hospital normalizado (Title Case via InitCap)
        INITCAP(TRIM(nome_hospital))                           AS nome_hospital,

        -- Geografia
        UPPER(TRIM(uf))                                        AS uf,
        TRIM(municipio)                                        AS municipio,
        TRIM(cod_municipio_ibge)                                AS cod_municipio_ibge,

        -- Classificação
        TRIM(especialidade)                                    AS especialidade,
        TRIM(tipo_atendimento)                                 AS tipo_atendimento,
        TRIM(carater_atendimento)                               AS carater_atendimento,

        -- Valores financeiros
        CAST(valor_total AS DOUBLE)                            AS valor_total,

        -- Métricas
        CAST(dias_permanencia AS INT)                          AS dias_permanencia,

        -- Paciente
        CASE
            WHEN TRIM(sexo) IS NULL OR TRIM(sexo) = '' THEN 'NI'
            ELSE UPPER(TRIM(sexo))
        END                                                    AS sexo,

        CASE
            WHEN CAST(idade AS INT) < 0 THEN NULL
            ELSE CAST(idade AS INT)
        END                                                    AS idade,

        -- Metadados de carga
        ingestion_at,
        source_file,

        -- Metadado dbt
        CURRENT_TIMESTAMP()                                    AS dbt_processed_at

    FROM source
)

SELECT * FROM cleaned
