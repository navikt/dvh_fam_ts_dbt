{{
    config(
        materialized = 'view'
    )
}}

--Hent ut ur data for input statistikk periode
with ur as (
    select /*+ parallel(64) */
        a.*
       ,{{ var('periode') }} as periode
    from {{ source ( 'fam_ef','fam_ts_ur_utbetaling' )}} a
    where to_char(posteringsdato, 'yyyymm') = {{ var('periode') }}
)
,

tid as (
    select aar_maaned, siste_dato_i_perioden
    from {{ source ( 'dt_kodeverk','dim_tid' )}}
    where gyldig_flagg = 1
    and dim_nivaa = 3 --Månedsnivå
)
,

--Per periode: alder på yngste barn og antall barn for hver aldersgruppe
periode_barn as (
    select
        fk_ts_vedtaksperioder
       ,min(floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12)) ybarn
       ,sum(case when floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 1 then 1 else 0 end) antbu1
       ,sum(case when floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 3 then 1 else 0 end) antbu3
       ,sum(case when floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 8 then 1 else 0 end) antbu8
       ,sum(case when floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 10 then 1 else 0 end) antbu10
       ,sum(case when floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 18 then 1 else 0 end) antbu18
    from fam_ts_barn_v2 barn
    
    join ur
    on ur.henvisning = barn.ekstern_behandling_id
    
    join tid
    on tid.aar_maaned = ur.periode

    join dt_person.dim_person dim_person_barn
    on dim_person_barn.fk_person1 = barn.fk_person1
    and tid.siste_dato_i_perioden between dim_person_barn.gyldig_fra_dato and dim_person_barn.gyldig_til_dato
    
    group by barn.fk_ts_vedtaksperioder
)
,

--Alle rader fra gjeldende ur og legg til vedtaks infomasjon
ur_vedtaksperiode as (
    select
        ur.pk_ur_utbetaling
       ,ur.fk_person1 --Mottaker
       ,ur.fk_dim_person --Mottaker
       ,ur.klassekode
       ,ur.henvisning
       ,ur.dato_utbet_fom
       ,ur.dato_utbet_tom
       ,ur.belop
       ,ur.periode
       ,tid.siste_dato_i_perioden
       ,periode_barn.ybarn
       ,periode_barn.antbu1
       ,periode_barn.antbu3
       ,periode_barn.antbu8
       ,periode_barn.antbu10
       ,periode_barn.antbu18

    from ur

    join tid
    on tid.aar_maaned = ur.periode

    left join {{ source('fam_ef','fam_ts_vedtaksperioder_v2') }} periode
    on ur.henvisning = periode.ekstern_behandling_id
    and ur.dato_utbet_fom between periode.fra_og_med and periode.til_og_med

    left join periode_barn
    on periode_barn.fk_ts_vedtaksperioder = periode.pk_ts_vedtaksperioder
)

select *
from ur_vedtaksperiode