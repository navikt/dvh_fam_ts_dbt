{{
    config(
        materialized='incremental',
        unique_key='ekstern_behandling_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['PK_TS_FAGSAK']
    )
}}

with ts_meta_data as (
  select * from {{ref ('ts_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
      fagsak_id                         VARCHAR2 PATH '$.fagsak_id',
      behandling_id                     VARCHAR2 PATH '$.behandling_id',
      ekstern_fagsak_id                 VARCHAR2 PATH '$.ekstern_fagsak_id',
      --ekstern_behandling_id             NUMBER PATH '$.ekstern_behandling_id',
      relatert_behandling_id            VARCHAR2 PATH '$.relatert_behandling_id',
      adressebeskyttelse                VARCHAR2 PATH '$.adressebeskyttelse',
      tidspunkt_vedtak                  VARCHAR2 PATH '$.tidspunkt_vedtak',
      person                            VARCHAR2 PATH '$.person',
      behandling_type                   VARCHAR2 PATH '$.behandling_type',
      behandling_arsak                  VARCHAR2 PATH '$.behandling_arsak',
      vedtak_resultat                   VARCHAR2 PATH '$.vedtak_resultat',
      stonadstype                       VARCHAR2 PATH '$.stonadstype',
      krav_mottatt                      DATE PATH '$.krav_mottatt'
    )
  ) j
),

final as (
  select
    p.fagsak_id
    ,p.behandling_id
    ,p.ekstern_fagsak_id
    ,p.ekstern_behandling_id
    ,p.relatert_behandling_id
    ,p.adressebeskyttelse
    ,TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + NUMTODSINTERVAL( tidspunkt_vedtak / 1000, 'SECOND') tidspunkt_vedtak
    ,p.pk_ts_meta_data as fk_ts_meta_data
    ,p.person
    ,p.behandling_type
    ,p.behandling_arsak
    ,p.vedtak_resultat
    ,p.stonadstype
    ,to_date(p.krav_mottatt,'yyyy-mm-dd') krav_mottatt
    ,nvl(ident.fk_person1, -1) as fk_person1
    ,p.endret_tid
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.person = ident.off_id
  and endret_tid between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_TS_FAGSAK
  ,FAGSAK_ID
  ,behandling_id
  ,ekstern_fagsak_id
  ,ekstern_behandling_id
  ,relatert_behandling_id
  ,adressebeskyttelse
  ,FK_TS_META_DATA
  , case wheN fk_person1 = -1  THEN person
      ELSE NULL
    END PERSON
  ,FK_PERSON1
  ,behandling_type
  ,behandling_arsak
  ,vedtak_resultat
  ,localtimestamp AS lastet_dato
  ,KRAV_MOTTATT
  ,tidspunkt_vedtak
  ,STONADSTYPE
  ,endret_tid
from final