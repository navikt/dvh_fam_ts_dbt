{{
    config(
        materialized='table'
    )
}}

with ts_meta_data as (
  SELECT distinct m.pk_ts_meta_data
    FROM {{ source ('fam_ef', 'fam_ts_meta_data_v2') }} m,
        JSON_TABLE(
            m.melding,
            '$'
            COLUMNS(
                type VARCHAR2(100) PATH '$.vedtaksperioder.lovverketsMålgruppe',
                vedtak_resultat VARCHAR2(100) PATH '$.vedtak_resultat'
            )
        ) j
    WHERE j.type = 'ENSLIG_FORSØRGER'
    OR j.vedtak_resultat = 'AVSLÅTT'
    AND m.endret_tid > nvl( (select max(endret_tid) from {{ source ('fam_ef', 'fam_ts_fagsak_v2') }}), m.endret_tid-1 )
)

select meta.*
from {{ source ('fam_ef', 'fam_ts_meta_data_v2') }} meta
join ts_meta_data
on meta.pk_ts_meta_data = ts_meta_data.pk_ts_meta_data