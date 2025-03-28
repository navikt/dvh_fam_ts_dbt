{{
    config(
        materialized='incremental',
        unique_key=['periode', 'gyldig_flagg'],
        incremental_strategy='delete+insert'
    )
}}

-- Hent fakta om mottakere og deres vedtaksperioder
with fakta as (
    select
        mottaker.*
       ,barn.ybarn
       ,barn.antbu1
       ,barn.antbu3
       ,barn.antbu8
       ,barn.antbu10
       ,barn.antbu18
       ,avslag.aarsak as avslag_aarsak
       ,opphor.aarsak as opphor_aarsak
    from {{ ref('ts_vedtaksperiode_mottaker_v2') }} mottaker

    -- Hent ut informasjon om barn for alle typer stønader, og en line per fk_ts_vedtaksperioder
    left join
    (
        select fk_ts_vedtaksperioder
              ,ekstern_behandling_id
              ,min(alder_barn) ybarn
              ,sum(bu1) antbu1
              ,sum(bu3) antbu3
              ,sum(bu8) antbu8
              ,sum(bu10) antbu10
              ,sum(bu18) antbu18
        from {{ ref('ts_vedtaksperiode_barn_v2') }}
        group by fk_ts_vedtaksperioder, ekstern_behandling_id
    ) barn
    on mottaker.ekstern_behandling_id = barn.ekstern_behandling_id
    and mottaker.fk_ts_vedtaksperioder = barn.fk_ts_vedtaksperioder

    -- Legge til informasjon om avslag om det finnes, og returnere kun en linje. Tar maks foreløpig.
    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_avslag_v2') }}
        group by fk_ts_fagsak
    ) avslag
    on mottaker.fk_ts_fagsak = avslag.fk_ts_fagsak
    
    -- Legge til informasjon om opphør om det finnes, og returnere kun en linje. Tar maks foreløpig.
    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_opphor_v2') }}
        group by fk_ts_fagsak
    ) opphor
    on mottaker.fk_ts_fagsak = opphor.fk_ts_fagsak
)
,

-- Return en linje per mottaker, per periode
fakta_per_mottaker as (
    select
        periode
       ,gyldig_flagg
       ,fk_person1 -- Mottaker
       ,fk_dim_person -- Mottaker
       ,siste_dato_i_perioden
       ,bosted_kommune_nr
       ,fk_dim_geografi
       ,bydel_kommune_nr       
       ,statsborgerskap
       ,fodeland
       ,sivilstatus_kode
       ,fk_dim_kjonn
       ,kjonn_kode
       ,fodsels_aar
       ,fodsels_mnd
       ,alder

       ,max(case when stonadstype = 'BARNETILSYN' then ekstern_behandling_id end) bt_ekstern_behandling_id
       ,max(case when stonadstype = 'LÆREMIDLER' then ekstern_behandling_id end) lm_ekstern_behandling_id

       ,max(case when stonadstype = 'BARNETILSYN' then fagsak_id end) bt_fagsak_id
       ,max(case when stonadstype = 'LÆREMIDLER' then fagsak_id end) lm_fagsak_id

       ,sum(case when stonadstype = 'BARNETILSYN' and to_char(dato_utbet_fom, 'yyyymm') = periode then belop else 0 end) tsotilbarn
       ,sum(case when stonadstype = 'BARNETILSYN' and to_char(dato_utbet_fom, 'yyyymm') < periode then belop else 0 end) tsotilbarn_etterbetalt

       ,sum(case when stonadstype = 'LÆREMIDLER' and to_char(dato_utbet_fom, 'yyyymm') = periode then belop else 0 end) tsolmidler
       ,sum(case when stonadstype = 'LÆREMIDLER' and to_char(dato_utbet_fom, 'yyyymm') < periode then belop else 0 end) tsolmidler_etterbetalt

       ,sum(belop) total_belop
       ,max(aktivitet) aaktivitet
       ,max(antall_barn) antall_barn
       ,max(lovverkets_maalgruppe) lovverkets_maalgruppe
       ,max(maalgruppe) maalgruppe
       ,max(studienivaa) studienivaa
       ,max(avslag_aarsak) avslag_aarsak
       ,max(opphor_aarsak) opphor_aarsak

       -- Return informasjon om barn så lenge det finnes i en av stønader
       ,max(ybarn) ybarn
       ,max(antbu1) antbu1
       ,max(antbu3) antbu3
       ,max(antbu8) antbu8
       ,max(antbu10) antbu10
       ,max(antbu18) antbu18
    from fakta
    group by
        periode
       ,gyldig_flagg
       ,fk_person1 -- Mottaker
       ,fk_dim_person -- Mottaker
       ,siste_dato_i_perioden
       ,bosted_kommune_nr
       ,fk_dim_geografi
       ,bydel_kommune_nr       
       ,statsborgerskap
       ,fodeland
       ,sivilstatus_kode
       ,fk_dim_kjonn
       ,kjonn_kode
       ,fodsels_aar
       ,fodsels_mnd
       ,alder
    having sum(belop) > 0 --Ikke return mottaker som har 0 beløp i periode
)

-- Velg alle kolonner fra fakta_per_mottaker
select a.*
      ,localtimestamp as lastet_dato
from fakta_per_mottaker a