{{
    config(
        materialized = 'table'
    )
}}

with fakta as (
    select
        mottaker.pk_ur_utbetaling
       ,mottaker.fk_person1 --Mottaker
       ,mottaker.fk_dim_person --Mottaker
       ,mottaker.klassekode
       ,mottaker.henvisning
       ,mottaker.dato_utbet_fom
       ,mottaker.dato_utbet_tom
       ,mottaker.belop
       ,mottaker.periode
       ,mottaker.siste_dato_i_perioden
       ,mottaker.ekstern_behandling_id
       ,mottaker.aktivitet
       ,mottaker.antall_barn
       ,mottaker.fra_og_med
       ,mottaker.til_og_med
       ,mottaker.lovverkets_maalgruppe
       ,mottaker.maalgruppe
       ,mottaker.studienivaa
       ,mottaker.fk_ts_fagsak
       ,mottaker.fk_ts_vedtaksperioder
       ,mottaker.bosted_kommune_nr
       ,mottaker.fk_dim_geografi
       ,mottaker.bydel_kommune_nr       
       ,mottaker.statsborgerskap
       ,mottaker.fodeland
       ,mottaker.sivilstatus_kode
       ,mottaker.fk_dim_kjonn
       ,mottaker.kjonn_kode
       ,barn.ybarn
       ,barn.antbu1
       ,barn.antbu3
       ,barn.antbu8
       ,barn.antbu10
       ,barn.antbu18
       ,fagsak.fagsak_id
       ,fagsak.fk_ts_meta_data
       ,fagsak.behandling_id
       ,fagsak.ekstern_fagsak_id
       ,fagsak.relatert_behandling_id
       ,fagsak.adressebeskyttelse
       ,fagsak.tidspunkt_vedtak
       ,fagsak.behandling_type
       ,fagsak.behandling_arsak
       ,fagsak.vedtak_resultat
       ,fagsak.stonadstype
       ,avslag.aarsak as avslag_aarsak
       ,opphor.aarsak as opphor_aarsak
    from {{ ref('ts_vedtaksperiode_mottaker_test') }} mottaker

    --Informasjon om barn og summere opp til en linje per periode
    left join
    (
        select fk_ts_vedtaksperioder, ekstern_behandling_id
              ,sum(bu1) antbu1
              ,sum(bu3) antbu3
              ,sum(bu8) antbu8
              ,sum(bu10) antbu10
              ,sum(bu18) antbu18
              ,min(alder_barn) ybarn
        from {{ ref('ts_vedtaksperiode_barn_test') }}
        group by fk_ts_vedtaksperioder, ekstern_behandling_id
    ) barn
    on mottaker.ekstern_behandling_id = barn.ekstern_behandling_id
    and mottaker.fk_ts_vedtaksperioder = barn.fk_ts_vedtaksperioder

    left join {{ source('fam_ef','fam_ts_fagsak_v2') }} fagsak
    on mottaker.fk_ts_fagsak = fagsak.pk_ts_fagsak

    --Legge til informasjon om avslag om det finnes, og returnere kun en linje. Tar maks foreløpig.
    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_avslag_v2') }}
        group by fk_ts_fagsak
    ) avslag
    on mottaker.fk_ts_fagsak = avslag.fk_ts_fagsak
    
    --Legge til informasjon om opphør om det finnes, og returnere kun en linje. Tar maks foreløpig.
    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_opphor_v2') }}
        group by fk_ts_fagsak
    ) opphor
    on mottaker.fk_ts_fagsak = opphor.fk_ts_fagsak
)
,

--Return en linje per mottaker
fakta_per_mottaker as (
    select
        periode
       ,fk_person1 --Mottaker
       ,fk_dim_person --Mottaker
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

       ,sum(case when stonadstype = 'BARNETILSYN' and to_char(dato_utbet_fom, 'yyyymm') = periode then belop end) tsotilbarn
       ,sum(case when stonadstype = 'BARNETILSYN' and to_char(dato_utbet_fom, 'yyyymm') < periode then belop end) tsotilbarn_etterbetalt

       ,sum(case when stonadstype = 'LÆREMIDLER' and to_char(dato_utbet_fom, 'yyyymm') = periode then belop end) tsolmidler
       ,sum(case when stonadstype = 'LÆREMIDLER' and to_char(dato_utbet_fom, 'yyyymm') < periode then belop end) tsolmidler_etterbetalt

       ,sum(belop) total_belop
       ,max(aktivitet) aaktivitet
       ,max(antall_barn) antall_barn
       ,max(lovverkets_maalgruppe) lovverkets_maalgruppe
       ,max(maalgruppe) maalgruppe
       ,max(studienivaa) studienivaa
       ,max(avslag_aarsak) avslag_aarsak
       ,max(opphor_aarsak) opphor_aarsak 
    from fakta
    group by
        periode
       ,fk_person1 --Mottaker
       ,fk_dim_person --Mottaker
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
)

select *
from fakta_per_mottaker