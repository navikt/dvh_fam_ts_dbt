{{
    config(
        materialized='incremental',
        unique_key='ekstern_behandling_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['PK_TS_FAGSAK']
    )
}}

with ts_meta_data as (
  select * from {{ref ('ts_meldinger_til_aa_pakke_ut_v2')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandling_id                     VARCHAR2 PATH '$.behandling_id',
      fagsak_id                         VARCHAR2 PATH '$.fagsak_id',
      ekstern_fagsak_id                 VARCHAR2 PATH '$.ekstern_fagsak_id',
      --ekstern_behandling_id             NUMBER PATH '$.ekstern_behandling_id',
      relatert_behandling_id            VARCHAR2 PATH '$.relatert_behandling_id',
      adressebeskyttelse                VARCHAR2 PATH '$.adressebeskyttelse',
      tidspunkt_vedtak                  VARCHAR2 PATH '$.tidspunkt_vedtak',
      soker_ident                       VARCHAR2 PATH '$.soker_ident',
      behandling_type                   VARCHAR2 PATH '$.behandling_type',
      behandling_arsak                  VARCHAR2 PATH '$.behandling_arsak',
      vedtak_resultat                   VARCHAR2 PATH '$.vedtak_resultat',
      stonadstype                       VARCHAR2 PATH '$.stonadstype'
      --opprettet_tid                     VARCHAR2 PATH '$.opprettet_tid'
    )
  ) j
),

final as (
  select
    nvl(ident.fk_person1, -1) as fk_person1
    ,p.pk_ts_meta_data as fk_ts_meta_data
    ,p.behandling_id  
    ,p.fagsak_id
    ,p.ekstern_fagsak_id
    ,p.ekstern_behandling_id
    ,p.relatert_behandling_id
    ,p.adressebeskyttelse
    ,TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + NUMTODSINTERVAL( tidspunkt_vedtak / 1000, 'SECOND') tidspunkt_vedtak
    ,p.soker_ident
    ,p.behandling_type
    ,p.behandling_arsak
    ,p.vedtak_resultat
    ,p.stonadstype
    ,p.opprettet_tid
    ,p.endret_tid
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.soker_ident = ident.off_id
  and endret_tid between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_TS_FAGSAK
  ,FK_PERSON1  
  ,FK_TS_META_DATA
  ,behandling_id  
  ,FAGSAK_ID
  ,ekstern_fagsak_id
  ,ekstern_behandling_id
  ,relatert_behandling_id
  ,adressebeskyttelse  
  ,tidspunkt_vedtak
  , case wheN fk_person1 = -1  THEN soker_ident
      ELSE NULL
    END soker_ident
  ,behandling_type
  ,behandling_arsak
  ,vedtak_resultat  
  ,STONADSTYPE
  ,opprettet_tid
  ,endret_tid  
  ,localtimestamp AS lastet_dato
from final