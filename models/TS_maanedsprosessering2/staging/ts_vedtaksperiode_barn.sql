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
       ,periode_barn.fk_person1_barn
       ,dim_person_barn.pk_dim_person as fk_dim_person_barn
       ,floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) alder_barn

    from ur

    join tid
    on tid.aar_maaned = ur.periode

    --Periode med alle barn fra perioden
    left join
    (
        select
            periode.ekstern_behandling_id
           ,periode.aktivitet
           ,periode.antall_barn
           ,periode.fra_og_med
           ,periode.til_og_med
           ,periode.lovverkets_maalgruppe
           ,periode.maalgruppe
           ,periode.studienivaa
           ,barn.fk_person1 as fk_person1_barn
        from {{ source('fam_ef','fam_ts_vedtaksperioder_v2') }} periode

        left join {{ source('fam_ef','fam_ts_barn_v2') }} barn
        on barn.fk_ts_vedtaksperioder = periode.pk_ts_vedtaksperioder
    ) periode_barn
    on ur.henvisning = periode_barn.ekstern_behandling_id
    and ur.dato_utbet_fom between periode_barn.fra_og_med and periode_barn.til_og_med

    left join dt_person.dim_person dim_person_barn
    on dim_person_barn.fk_person1 = periode_barn.fk_person1_barn
    and tid.siste_dato_i_perioden between dim_person_barn.gyldig_fra_dato and dim_person_barn.gyldig_til_dato
    and dim_person_barn.skjermet_kode = 0 --Filtrer vekk kode67    
)

select 
    a.*
   ,case when alder_barn < 1 then 1 else 0 end bu1
   ,case when alder_barn < 3 then 1 else 0 end bu3
   ,case when alder_barn < 8 then 1 else 0 end bu8
   ,case when alder_barn < 10 then 1 else 0 end bu10
   ,case when alder_barn < 18 then 1 else 0 end bu18
from ur_vedtaksperiode a