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
      nested                path '$.vilkarsvurderinger[*]' columns (
          resultat          path '$.resultat'
          ,nested                path '$.vilkår[*]' columns (
            vilkaar_resultat     path '$.resultat'
            ,nested                path '$.vurderinger[*]' columns (
              vurdering             varchar2 path '$[*]'
          )
        )
      )
    )
  )j
  where json_value (melding, '$.vilkarsvurderinger.size()' )> 0
),

final as (
  select
    p.resultat,
    p.ekstern_behandling_id,
    p.vilkaar_resultat,
    p.vurdering,
    pk_ts_FAGSAK as FK_ts_FAGSAK
  from pre_final p
  join ts_fagsak b
  on p.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_vilkarsvurderinger,
  FK_ts_FAGSAK,
  resultat,
  vilkaar_resultat,
  vurdering,
  ekstern_behandling_id,
  localtimestamp AS LASTET_DATO
from final
