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
       ,antbu1
       ,antbu3
       ,antbu8
       ,antbu10
       ,antbu18
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

    --Periode med alle barn fra perioden
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

    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_avslag_v2') }}
        group by fk_ts_fagsak
    ) avslag
    on mottaker.fk_ts_fagsak = avslag.fk_ts_fagsak

    left join
    (
        select fk_ts_fagsak, max(aarsak) aarsak
        from {{ source('fam_ef','fam_ts_arsaker_opphor_v2') }}
        group by fk_ts_fagsak
    ) opphor
    on mottaker.fk_ts_fagsak = opphor.fk_ts_fagsak
)

select
    periode
   ,fk_person1 --Mottaker
   ,fk_dim_person --Mottaker
   ,pk_ur_utbetaling
   ,klassekode
   ,henvisning
   ,dato_utbet_fom
   ,dato_utbet_tom
   ,belop
   ,siste_dato_i_perioden
   ,fk_ts_fagsak
   ,ekstern_behandling_id
   ,aktivitet
   ,antall_barn
   ,fra_og_med
   ,til_og_med
   ,lovverkets_maalgruppe
   ,maalgruppe
   ,studienivaa
   ,fk_ts_vedtaksperioder
   ,ybarn
   ,antbu1
   ,antbu3
   ,antbu8
   ,antbu10
   ,antbu18
   ,fagsak_id
   ,bosted_kommune_nr
   ,fk_dim_geografi
   ,bydel_kommune_nr       
   ,statsborgerskap
   ,fodeland
   ,sivilstatus_kode
   ,fk_dim_kjonn
   ,kjonn_kode
from fakta