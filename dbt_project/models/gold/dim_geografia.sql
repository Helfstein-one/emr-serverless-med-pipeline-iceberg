{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

{#
============================================================
dim_geografia.sql — Dimensão de Geografia
============================================================
Dimensão geográfica com UFs, municípios, código IBGE
e mapeamento de região.
============================================================
#}

WITH municipios AS (
    SELECT DISTINCT
        cod_municipio_ibge,
        municipio,
        uf
    FROM {{ ref('stg_internacoes_dedup') }}
    WHERE cod_municipio_ibge IS NOT NULL
),

enriched AS (
    SELECT
        -- Surrogate Key baseada no código IBGE
        CAST(cod_municipio_ibge AS BIGINT)          AS sk_geografia,

        cod_municipio_ibge,
        municipio                                   AS nome_municipio,
        uf,

        -- Mapeamento de Região por UF
        CASE
            WHEN uf IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
            WHEN uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
            WHEN uf IN ('DF', 'GO', 'MS', 'MT') THEN 'Centro-Oeste'
            WHEN uf IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
            WHEN uf IN ('PR', 'RS', 'SC') THEN 'Sul'
            ELSE 'Não Identificado'
        END                                         AS regiao,

        -- Macro região para agrupamento
        CASE
            WHEN uf IN ('SP', 'RJ', 'MG', 'ES', 'PR', 'RS', 'SC') THEN 'Sul-Sudeste'
            WHEN uf IN ('BA', 'PE', 'CE', 'MA', 'PB', 'AL', 'PI', 'RN', 'SE') THEN 'Nordeste'
            WHEN uf IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
            WHEN uf IN ('DF', 'GO', 'MS', 'MT') THEN 'Centro-Oeste'
            ELSE 'Outros'
        END                                         AS macro_regiao

    FROM municipios
)

SELECT * FROM enriched
