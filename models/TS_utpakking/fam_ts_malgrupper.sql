{{
    config(
        materialized='incremental',
        unique_key='ekstern_behandling_id',
        incremental_strategy='delete+insert'
    )
}}

with ts_meta_data as (
  select * from {{ref ('ts_meldinger_til_aa_pakke_ut')}}
),

ts_fagsak as (
  select * from {{ref ('fam_ts_fagsak')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
      nested                path '$.malgrupper[*]' columns (
        resultat              varchar2 path '$.resultat'
        ,type                 varchar2 path '$.type'
        )
    )
  ) j
),

final as (
  select
    p.RESULTAT,
    p.type,
    p.ekstern_behandling_id,
    pk_ts_FAGSAK as FK_ts_FAGSAK
  from pre_final p
  join ts_fagsak b
  on p.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_malgrupper,
  FK_ts_FAGSAK,
  RESULTAT,
  type,
  ekstern_behandling_id,
  localtimestamp AS LASTET_DATO
from final
where type = 'OVERGANGSSTØNAD'