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
      nested                path '$.utbetalinger[*]' columns (
          belop                           NUMBER path '$.beløp'
          ,BELOP_ER_BEGRENSET_AV_MAKSSATS VARCHAR2 path '$.beløpErBegrensetAvMakssats'
          ,fra_og_med                     DATE path '$.fraOgMed'
          ,makssats                       NUMBER path '$.makssats'
          ,til_og_med                     DATE path '$.tilOgMed'
          ,type                           varchar2 path '$.type'
      )
    )
  ) j
  --where json_value (melding, '$.utbetalinger.size()' )> 0
),

final as (
  select
      p.belop
      ,p.BELOP_ER_BEGRENSET_AV_MAKSSATS
      ,p.fra_og_med
      ,p.makssats
      ,p.til_og_med
      ,p.type
      ,p.ekstern_behandling_id
      ,pk_ts_FAGSAK as FK_ts_FAGSAK
    from pre_final p
    join ts_fagsak b
    on p.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_utbetalinger,
  FK_ts_FAGSAK,
  belop
  ,case when BELOP_ER_BEGRENSET_AV_MAKSSATS = 'false' THEN 0
      ELSE 1
    END BELOP_ER_BEGRENSET_AV_MAKSSATS
  ,fra_og_med
  ,makssats
  ,til_og_med
  ,type
  ,ekstern_behandling_id
  ,localtimestamp AS LASTET_DATO
from final
where fra_og_med IS NOT NULL