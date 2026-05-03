{{
    config(
        materialized='table',
        file_format='iceberg',
        partition_by=[{'field': 'data_internacao', 'data_type': 'date', 'granularity': 'month'}]
    )
}}

{#
============================================================
fct_atendimentos.sql — Tabela de Fatos (Star Schema)
============================================================
Fato central do modelo dimensional.
Junta as dimensões de tempo, geografia e especialidade
com as métricas de atendimento (valor, dias, contagem).
============================================================
#}

WITH atendimentos AS (
    SELECT * FROM {{ ref('stg_internacoes_dedup') }}
),

dim_t AS (
    SELECT * FROM {{ ref('dim_tempo') }}
),

dim_g AS (
    SELECT * FROM {{ ref('dim_geografia') }}
),

dim_e AS (
    SELECT * FROM {{ ref('dim_especialidade') }}
),

facts AS (
    SELECT
        -- Surrogate Key do fato
        ABS(HASH(a.id_internacao))                  AS sk_atendimento,

        -- Chave natural
        a.id_internacao,

        -- Foreign Keys para dimensões
        dt_int.sk_tempo                             AS sk_tempo_internacao,
        dt_alt.sk_tempo                             AS sk_tempo_alta,
        dg.sk_geografia                             AS sk_geografia,
        de.sk_especialidade                         AS sk_especialidade,

        -- Datas (para particionamento e consultas diretas)
        a.data_internacao,
        a.data_alta,

        -- Atributos descritivos (desnormalizados para performance)
        a.cid_principal,
        a.cid_secundario,
        a.tipo_atendimento,
        a.carater_atendimento,
        a.sexo,
        a.idade,

        -- ========== MÉTRICAS ==========
        a.valor_total,
        a.dias_permanencia,

        -- Custo diário médio
        CASE
            WHEN a.dias_permanencia > 0
                THEN ROUND(a.valor_total / a.dias_permanencia, 2)
            ELSE a.valor_total
        END                                         AS custo_diario_medio,

        -- Faixa etária
        CASE
            WHEN a.idade IS NULL THEN 'Não Informado'
            WHEN a.idade < 1    THEN '< 1 ano'
            WHEN a.idade < 12   THEN '1-11 anos'
            WHEN a.idade < 18   THEN '12-17 anos'
            WHEN a.idade < 30   THEN '18-29 anos'
            WHEN a.idade < 45   THEN '30-44 anos'
            WHEN a.idade < 60   THEN '45-59 anos'
            WHEN a.idade < 75   THEN '60-74 anos'
            ELSE '75+ anos'
        END                                         AS faixa_etaria,

        -- Classificação de permanência
        CASE
            WHEN a.dias_permanencia <= 1  THEN 'Curta (<=1 dia)'
            WHEN a.dias_permanencia <= 5  THEN 'Média (2-5 dias)'
            WHEN a.dias_permanencia <= 14 THEN 'Longa (6-14 dias)'
            ELSE 'Muito Longa (15+ dias)'
        END                                         AS classificacao_permanencia,

        -- Faixa de valor
        CASE
            WHEN a.valor_total < 1000    THEN 'Baixo (< R$1.000)'
            WHEN a.valor_total < 5000    THEN 'Médio (R$1.000-5.000)'
            WHEN a.valor_total < 20000   THEN 'Alto (R$5.000-20.000)'
            ELSE 'Muito Alto (> R$20.000)'
        END                                         AS faixa_valor,

        -- Metadados
        a.ingestion_at,
        CURRENT_TIMESTAMP()                         AS dbt_loaded_at

    FROM atendimentos a

    -- Join com dimensão de tempo (data da internação)
    LEFT JOIN dim_t dt_int
        ON CAST(DATE_FORMAT(a.data_internacao, 'yyyyMMdd') AS INT) = dt_int.sk_tempo

    -- Join com dimensão de tempo (data da alta)
    LEFT JOIN dim_t dt_alt
        ON CAST(DATE_FORMAT(a.data_alta, 'yyyyMMdd') AS INT) = dt_alt.sk_tempo

    -- Join com dimensão de geografia
    LEFT JOIN dim_g dg
        ON CAST(a.cod_municipio_ibge AS BIGINT) = dg.sk_geografia

    -- Join com dimensão de especialidade
    LEFT JOIN dim_e de
        ON a.especialidade = de.nome_especialidade
)

SELECT * FROM facts
