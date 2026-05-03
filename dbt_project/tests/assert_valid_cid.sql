{#
============================================================
assert_valid_cid.sql — Teste Singular
============================================================
Valida que todos os CIDs principais seguem o padrão
do CID-10: letra + 2 dígitos + ponto + 1+ dígitos
Exemplo válido: J18.9, I50.0, A09.0
Retorna registros que FALHAM na validação.
============================================================
#}

SELECT
    id_internacao,
    cid_principal
FROM {{ ref('stg_internacoes_dedup') }}
WHERE cid_principal IS NOT NULL
  AND cid_principal NOT RLIKE '^[A-Z][0-9]{2}\\.[0-9]+$'
