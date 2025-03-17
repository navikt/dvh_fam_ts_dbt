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

ts_vedtaksperioder as (
  select * from {{ref ('fam_ts_vedtaksperioder_v2')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
        nested path '$.vedtaksperioder[*]' columns (fra_og_med DATE path '$.fom', til_og_med DATE path '$.tom',
        nested path '$.barn.barn[*]' columns (fnr varchar2 path '$.fnr')
        )
    )
  ) j
),

til_fk_person1 as (
    select
    fra_og_med,
    til_og_med,
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
    v.pk_ts_vedtaksperioder as FK_ts_vedtaksperioder
  from til_fk_person1 t
  join ts_vedtaksperioder v
  on t.ekstern_behandling_id = v.ekstern_behandling_id
  and t.fra_og_med = v.fra_og_med
  and t.til_og_med = v.til_og_med
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_ts_barn,
  FK_TS_VEDTAKSPERIODER,
  case wheN fk_person1 = -1  THEN fnr
      ELSE NULL
    END fnr,
  FK_PERSON1,
  ekstern_behandling_id,
  localtimestamp AS LASTET_DATO
from final
where fnr is not null