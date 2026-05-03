{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

{#
============================================================
dim_tempo.sql — Dimensão de Tempo
============================================================
Gera uma dimensão de datas baseada no range de datas
encontrado nos atendimentos Silver.
Atributos: ano, trimestre, mês, dia, dia da semana,
fim de semana, período fiscal.
============================================================
#}

WITH date_range AS (
    SELECT
        MIN(data_internacao) AS min_date,
        MAX(data_alta)       AS max_date
    FROM {{ ref('stg_internacoes_dedup') }}
),

date_spine AS (
    SELECT
        EXPLODE(
            SEQUENCE(
                (SELECT min_date FROM date_range),
                (SELECT max_date FROM date_range),
                INTERVAL 1 DAY
            )
        ) AS data
)

SELECT
    -- Surrogate Key (YYYYMMDD)
    CAST(DATE_FORMAT(data, 'yyyyMMdd') AS INT)     AS sk_tempo,

    -- Data
    data                                            AS data,

    -- Componentes do calendário
    YEAR(data)                                      AS ano,
    QUARTER(data)                                   AS trimestre,
    MONTH(data)                                     AS mes,
    DAY(data)                                       AS dia,
    DAYOFWEEK(data)                                 AS dia_semana,
    DATE_FORMAT(data, 'EEEE')                       AS nome_dia_semana,
    DATE_FORMAT(data, 'MMMM')                       AS nome_mes,

    -- Indicadores
    CASE
        WHEN DAYOFWEEK(data) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                             AS is_fim_semana,

    -- Semestre
    CASE
        WHEN MONTH(data) <= 6 THEN 1
        ELSE 2
    END                                             AS semestre,

    -- Período fiscal (considerando ano fiscal = ano calendário)
    CONCAT('FY', CAST(YEAR(data) AS STRING))        AS periodo_fiscal,

    -- Semana do ano (ISO)
    WEEKOFYEAR(data)                                AS semana_ano

FROM date_spine
