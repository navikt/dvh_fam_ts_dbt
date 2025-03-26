{{
    config(
        materialized = 'view'
    )
}}

-- Hent ut ur data for input statistikk periode
with mottaker as (
    select  
        periode
       ,fk_person1 -- Mottaker
       ,fk_dim_person -- Mottaker
       ,siste_dato_i_perioden
       ,ekstern_behandling_id
       ,FK_TS_VEDTAKSPERIODER
    from {{ ref('ts_vedtaksperiode_mottaker_v2')}}
    group by
        periode
       ,fk_person1 -- Mottaker
       ,fk_dim_person -- Mottaker
       ,siste_dato_i_perioden
       ,ekstern_behandling_id
       ,FK_TS_VEDTAKSPERIODER
)
,

-- Alle rader fra gjeldende ur og legg til vedtaksinformasjon
barn_vedtaksperiode as (
    select
       mottaker.fk_person1 -- Mottaker
       ,mottaker.fk_dim_person -- Mottaker
       ,mottaker.periode
       ,mottaker.siste_dato_i_perioden
       ,mottaker.ekstern_behandling_id
       ,mottaker.fk_ts_vedtaksperioder
       ,barn.pk_ts_barn as fk_ts_barn
       ,barn.fk_person1 as fk_person1_barn
       ,dim_person_barn.pk_dim_person as fk_dim_person_barn
       ,floor(months_between(mottaker.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) alder_barn

    from mottaker

    -- Periode med alle barn fra perioden
    left join {{ source('fam_ef','fam_ts_barn_v2') }} barn
    on barn.ekstern_behandling_id = mottaker.ekstern_behandling_id
    and barn.fk_ts_vedtaksperioder = mottaker.fk_ts_vedtaksperioder

    -- Legg til personinformasjon for barn
    left join {{ source('dt_person','dim_person') }} dim_person_barn
    on dim_person_barn.fk_person1 = barn.fk_person1
    and mottaker.siste_dato_i_perioden between dim_person_barn.gyldig_fra_dato and dim_person_barn.gyldig_til_dato
    and dim_person_barn.skjermet_kode = 0 -- Filtrer vekk kode67    
)

-- Velg alle kolonner fra barn_vedtaksperiode og beregn aldersgrupper for barn
select 
    a.*
   ,case when alder_barn < 1 then 1 else 0 end bu1
   ,case when alder_barn < 3 then 1 else 0 end bu3
   ,case when alder_barn < 8 then 1 else 0 end bu8
   ,case when alder_barn < 10 then 1 else 0 end bu10
   ,case when alder_barn < 18 then 1 else 0 end bu18
from barn_vedtaksperiode a