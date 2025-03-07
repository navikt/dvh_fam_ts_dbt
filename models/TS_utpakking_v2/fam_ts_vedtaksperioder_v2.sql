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
      nested                path '$.vedtaksperioder[*]' columns (
          aktivitet               VARCHAR2 path '$.aktivitet'
          ,antall_barn            NUMBER path '$.antallBarn'
          ,fra_og_med             DATE path '$.fom'
          ,lovverkets_maalgruppe  VARCHAR2 path '$.lovverketsMålgruppe'
          ,maalgruppe             VARCHAR2 path '$.målgruppe'
          ,studentnivaa           VARCHAR2 path '$.studentnivå'
          ,til_og_med             DATE path '$.tom'
      )
    )
  ) j
  where json_value (melding, '$.vedtaksperioder.size()' )> 0
),

final as (
  select
      p.aktivitet
      ,p.antall_barn
      ,p.fra_og_med
      ,p.lovverkets_maalgruppe
      ,p.maalgruppe
      ,p.studentnivaa
      ,p.til_og_med
      ,p.ekstern_behandling_id
      ,pk_ts_FAGSAK as FK_ts_FAGSAK
    from pre_final p
    join ts_fagsak b
    on p.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_vedtaksperioder,
  FK_ts_FAGSAK
  ,aktivitet
  ,antall_barn
  ,fra_og_med
  ,lovverkets_maalgruppe
  ,maalgruppe
  ,studentnivaa
  ,til_og_med
  ,ekstern_behandling_id
  ,localtimestamp AS LASTET_DATO
from final