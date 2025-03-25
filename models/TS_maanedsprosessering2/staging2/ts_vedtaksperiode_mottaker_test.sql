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
       ,periode.ekstern_behandling_id
       ,periode.aktivitet
       ,periode.antall_barn
       ,periode.fra_og_med
       ,periode.til_og_med
       ,periode.lovverkets_maalgruppe
       ,periode.maalgruppe
       ,periode.studienivaa
       ,periode.fk_ts_fagsak
       ,periode.fk_ts_vedtaksperioder
       ,dim_person.bosted_kommune_nr
       ,dim_geografi.pk_dim_geografi as fk_dim_geografi
       ,dim_geografi.bydel_kommune_nr       
       ,dim_person.statsborgerskap
       ,dim_person.fodeland
       ,dim_person.sivilstatus_kode
       ,dim_person.fk_dim_kjonn
       ,dim_kjonn.kjonn_kode

    from ur

    join tid
    on tid.aar_maaned = ur.periode

    left join {{ source('fam_ef','fam_ts_vedtaksperioder_v2') }} periode
    on ur.henvisning = periode.ekstern_behandling_id
    and ur.dato_utbet_fom between periode.fra_og_med and periode.til_og_med

    left join {{ source('dt_person','dim_person') }} dim_person
    on dim_person.fk_person1 = ur.fk_person1
    and tid.siste_dato_i_perioden between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
    and dim_person.skjermet_kode = 0 --Filtrer vekk kode67

    left join {{ source('dt_kodeverk','dim_geografi') }} dim_geografi
    on dim_geografi.pk_dim_geografi = dim_person.fk_dim_geografi_bosted

    left join {{ source('dt_kodeverk','dim_kjonn') }} dim_kjonn
    on dim_kjonn.pk_dim_kjonn = dim_person.fk_dim_kjonn
)

select *
from ur_vedtaksperiode