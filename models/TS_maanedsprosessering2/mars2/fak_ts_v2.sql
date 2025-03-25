{{
    config(
        materialized = 'table'
    )
}}

with fakta as (
    select
        mottaker.*
       ,fagsak.fagsak_id
    from {{ ref('ts_vedtaksperiode_mottaker') }} mottaker

    left join {{ source('fam_ef','fam_ts_fagsak_v2') }} fagsak
    on mottaker.fk_ts_fagsak = fagsak.pk_ts_fagsak
)