-- Uendret fra v1
{{
    config(
        materialized='incremental',
        unique_key='ekstern_behandling_id',
        incremental_strategy='delete+insert'
    )
}}

with ts_meta_data as (
  select * from {{ref ('ts_meldinger_til_aa_pakke_ut_v2')}}
),

ts_fagsak as (
  select * from {{ref ('fam_ts_fagsak_v2')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
      nested               path '$.arsaker_avslag.årsaker[*]' columns (
        aarsak             varchar2 path '$[*]'
        )
    )
  ) j
  where json_value (melding, '$.arsaker_avslag.årsaker.size()' )> 0
),

final as (
  select
    p.aarsak,
    p.ekstern_behandling_id,
    pk_ts_FAGSAK as FK_ts_FAGSAK
  from pre_final p
  join ts_fagsak b
  on p.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_arsaker_avslag,
  FK_ts_FAGSAK,
  aarsak,
  ekstern_behandling_id,
  localtimestamp AS LASTET_DATO
from final