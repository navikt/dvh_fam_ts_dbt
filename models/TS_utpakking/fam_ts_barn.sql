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
      nested                path '$.barn[*]' columns (
        fnr              varchar2 path '$.fnr'
      )
    )
  ) j
),

til_fk_person1 as (
    select
    ekstern_behandling_id,
    fnr,
    nvl(ident.fk_person1, -1) fk_person1
  from pre_final  p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.fnr = ident.off_id
  and p.endret_tid between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
),

final as (
  select
    t.fnr,
    t.fk_person1,
    t.ekstern_behandling_id,
    b.pk_ts_FAGSAK as FK_ts_FAGSAK
  from til_fk_person1 t
  join ts_fagsak b
  on t.ekstern_behandling_id = b.ekstern_behandling_id
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_barn,
  FK_ts_FAGSAK,
  case wheN fk_person1 = -1  THEN fnr
      ELSE NULL
    END fnr,
  FK_PERSON1,
  ekstern_behandling_id,
  localtimestamp AS LASTET_DATO
from final
