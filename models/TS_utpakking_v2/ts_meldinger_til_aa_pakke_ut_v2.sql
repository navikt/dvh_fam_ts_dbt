{{
    config(
        materialized='table'
    )
}}

with ts_meta_data as (
  SELECT DISTINCT 
    m.pk_ts_meta_data
  FROM 
    {{ source('fam_ef', 'fam_ts_meta_data_v2') }} m,
    JSON_TABLE(
      m.melding,
      '$'
      COLUMNS (
        vedtak_resultat VARCHAR2(100) PATH '$.vedtak_resultat',
        NESTED PATH '$.vedtaksperioder[*]'
        COLUMNS (
          lovverkets_maalgruppe VARCHAR2(100) PATH '$.lovverketsMålgruppe'
        )
      )
    ) j
  /*WHERE 
    (j.lovverkets_maalgruppe = 'ENSLIG_FORSØRGER' OR j.vedtak_resultat = 'AVSLÅTT')
    AND m.endret_tid > NVL(
      (SELECT MAX(endret_tid) FROM {{ source('fam_ef', 'fam_ts_fagsak_v2') }}), 
      m.endret_tid - 1
    )*/
  where (j.lovverkets_maalgruppe = 'ENSLIG_FORSØRGER' or j.vedtaks_resultat in ('AVSLÅTT', 'OPPHØRT'))
    and endret_tid > sysdate - 30
    and m.ekstern_behandling_id not in
        (
            select ekstern_behandling_id
            from {{ source('fam_ef', 'fam_ts_fagsak_v2') }}
        )
)

SELECT 
  meta.*
FROM 
  {{ source('fam_ef', 'fam_ts_meta_data_v2') }} meta
JOIN 
  ts_meta_data
ON 
  meta.pk_ts_meta_data = ts_meta_data.pk_ts_meta_data