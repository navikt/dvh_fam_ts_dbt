{{
    config(
        materialized='table'
    )
}}

with ts_meta_data as (
  SELECT distinct m.pk_ts_meta_data
    FROM {{ source ('fam_ef', 'fam_ts_meta_data') }} m,
        JSON_TABLE(
            m.melding,
            '$.malgrupper.type[*]'
            COLUMNS(
                type VARCHAR2(100) PATH '$'
            )
        ) j
    WHERE j.type = 'OVERGANGSSTØNAD'
    AND endret_tid > nvl( (select max(endret_tid) from {{ source ('fam_ef', 'fam_ts_fagsak') }}), endret_tid-1 )
    --where opprettet_tid >= sysdate - 30 and ekstern_behandling_id not in (
      --select ekstern_behandling_id from {{ source ('fam_ef', 'fam_ts_fagsak') }})
        --and endret_tid <= (select max(endret_tid) from {{ this }})
)

select meta.*
from {{ source ('fam_ef', 'fam_ts_meta_data') }} meta
join ts_meta_data
on meta.pk_ts_meta_data = ts_meta_data.pk_ts_meta_data