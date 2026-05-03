{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

{#
============================================================
dim_especialidade.sql — Dimensão de Especialidade Médica
============================================================
Dimensão de especialidades com categorização por área
da medicina (Clínica, Cirúrgica, Diagnóstica, etc.)
============================================================
#}

WITH especialidades AS (
    SELECT DISTINCT
        especialidade AS nome_especialidade
    FROM {{ ref('stg_internacoes_dedup') }}
    WHERE especialidade IS NOT NULL
),

categorized AS (
    SELECT
        -- Surrogate Key (hash determinístico)
        ABS(HASH(nome_especialidade))               AS sk_especialidade,

        nome_especialidade,

        -- Categorização por área médica
        CASE
            WHEN nome_especialidade IN ('Cardiologia', 'Pneumologia', 'Gastroenterologia',
                                        'Neurologia', 'Nefrologia', 'Endocrinologia',
                                        'Infectologia', 'Geriatria', 'Dermatologia')
                THEN 'Clínica Médica'
            WHEN nome_especialidade IN ('Cirurgia Geral', 'Ortopedia', 'Urologia')
                THEN 'Cirúrgica'
            WHEN nome_especialidade IN ('Pediatria')
                THEN 'Materno-Infantil'
            WHEN nome_especialidade IN ('Ginecologia')
                THEN 'Saúde da Mulher'
            WHEN nome_especialidade IN ('Oncologia')
                THEN 'Oncologia'
            ELSE 'Outras'
        END                                         AS categoria,

        -- Indicador de especialidade cirúrgica
        CASE
            WHEN nome_especialidade IN ('Cirurgia Geral', 'Ortopedia', 'Urologia')
                THEN TRUE
            ELSE FALSE
        END                                         AS is_cirurgica

    FROM especialidades
)

SELECT * FROM categorized
