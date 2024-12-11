{{
    config(
        materialized = 'incremental',
        unique_key = 'periode',
        incremental_strategy='delete+insert',
    )
}}


with ts_mottaker_data as (
  SELECT
    to_char(TID.DATO,'YYYYMM') PERIODE,
    UR.FK_PERSON1,
    UR.FK_DIM_PERSON,
    dim_geografi.pk_dim_geografi FK_DIM_GEOGRAFI,
    DIM_PERSON.BOSTED_KOMMUNE_NR KOMMUNE_NR,
    DIM_GEOGRAFI.BYDEL_NR,
    DIM_PERSON.STATSBORGERSKAP,
    DIM_PERSON.FODELAND,
    DIM_PERSON.SIVILSTATUS_KODE,
    max(HENVISNING) BEHANDLING_ID,
    ur.klassekode,
    SUM(CASE WHEN to_char(TID_FOM.DATO,'YYYYMM')= to_char(TID.DATO,'YYYYMM') THEN UR.BELOP ELSE 0 END)  TSOTILBARN,
    SUM(CASE WHEN to_char(TID_FOM.DATO,'YYYYMM')< to_char(TID.DATO,'YYYYMM') THEN UR.BELOP ELSE 0 END) TSOTILBARN_ETTERBETALT,
    --ts.endret_tid --melding.aktiviteter.type
    --UR.LASTET_DATO, TID_FOM.DATO UTBET_FOM,TID_TOM.DATO UTBET_TOM,
    to_char(DIM_PERSON.FODT_DATO,'YYYY') FODSELS_AAR,
    to_char(DIM_PERSON.FODT_DATO,'MM') FODSELS_MND,
    floor(months_between(tid.dato, dim_person.fodt_dato)/12) ALDER,
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode KJONN,
    AKTIVITET,
    AKTIVITET_2,
    BARN.ANTBARN,
    BARN.ANTBU1,
    BARN.ANTBU3,
    BARN.ANTBU8,
    BARN.ANTBU10,
    BARN.ANTBU18
    from {{ source ('ur', 'fak_ur_utbetaling') }} UR
    JOIN {{ source ('ur', 'DIM_KONTO_UR') }} KONTO
    ON UR.FK_DIM_KONTO_UR = KONTO.PK_DIM_KONTO_UR
    JOIN {{ source ('kode_verk', 'dim_tid') }} TID
    ON TID.PK_DIM_TID = UR.FK_DIM_TID_DATO_POSTERT_UR
    JOIN {{ source ('kode_verk', 'dim_tid') }} TID_FOM
    ON TID_FOM.PK_DIM_TID = UR.FK_DIM_TID_DATO_UTBET_FOM
    JOIN {{ source ('kode_verk', 'dim_tid') }} TID_TOM
    ON TID_TOM.PK_DIM_TID = UR.FK_DIM_TID_DATO_UTBET_TOM
    JOIN {{ source ('kode_verk', 'dim_tid') }} TID_VAL
    ON UR.FK_DIM_TID_VALUTERINGSDATO = TID_VAL.PK_DIM_TID
    JOIN {{ source ('dt_person_arena', 'DIM_PERSON') }} dim_person
    on ur.fk_dim_person = dim_person.pk_dim_person
    and dim_person.k67_flagg = 0

    JOIN {{ source ('arena_stonad', 'dim_geografi') }} dim_geografi
    on dim_person.fk_dim_geografi_bosted=dim_geografi.pk_dim_geografi

    JOIN {{ source ('arena_stonad', 'dim_kjonn') }} dim_kjonn
    on dim_person.fk_dim_kjonn=dim_kjonn.pk_dim_kjonn

    LEFT OUTER JOIN {{ source ('fam_ef', 'fam_ts_fagsak') }} FAGSAK ON
    FAGSAK.ekstern_behandling_id=UR.HENVISNING
    LEFT OUTER JOIN
    (
    SELECT fk_ts_fagsak, MIN(TYPE) AKTIVITET, MAX(TYPE) AKTIVITET_2, COUNT(*) ANTALL_AKTIVITET FROM
    fam_ts_aktiviteter
    WHERE
    RESULTAT='OPPFYLT'
    GROUP BY fk_ts_fagsak
    ) AKT ON
    akt.fk_ts_fagsak=fagsak.pk_ts_fagsak

    LEFT OUTER JOIN {{ ref ('fak_ts_mottaker_barn') }} BARN ON
    UR.FK_PERSON1=BARN.FK_PERSON1
    AND to_char(TID.DATO,'YYYYMM')=BARN.PERIODE

    where konto.HOVEDKONTONR IN
    ('777') -- Tilleggsstønader

    and periode = {{ var ('periode')}}

    and UR.FK_DIM_TID_DATO_POSTERT_UR >= 20240501
    AND UR.KLASSEKODE IN (--'TSTBASISP4-OP',
    'TSTBASISP2-OP','TSTBASISP3-OP'--,'TSTBASISP5-OP'
    )
    --and ur.henvisning in ('8','34','37')
    AND LENGTH(UR.HENVISNING)>1
    --GROUP BY UR.klassekode, UR.FK_PERSON1,UR.HENVISNING,konto.underkonto_navn
    GROUP BY
    UR.FK_PERSON1,
    UR.FK_DIM_PERSON,
    dim_geografi.pk_dim_geografi,
    DIM_PERSON.BOSTED_KOMMUNE_NR,
    DIM_PERSON.STATSBORGERSKAP,
    DIM_PERSON.FODELAND,
    DIM_GEOGRAFI.BYDEL_NR,
    DIM_PERSON.SIVILSTATUS_KODE,
    ur.klassekode,
    to_char(TID.DATO,'YYYYMM'),
    --ts.endret_tid --melding.aktiviteter.type
    --UR.LASTET_DATO, TID_FOM.DATO UTBET_FOM,TID_TOM.DATO UTBET_TOM,
    to_char(DIM_PERSON.FODT_DATO,'YYYY'),
    to_char(DIM_PERSON.FODT_DATO,'MM'),
    floor(months_between(tid.dato, dim_person.fodt_dato)/12),
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode,
    AKTIVITET,
    AKTIVITET_2,
    BARN.ANTBARN,
    BARN.ANTBU1,
    BARN.ANTBU3,
    BARN.ANTBU8,
    BARN.ANTBU10,
    BARN.ANTBU18
)

select
  PERIODE
  ,FK_PERSON1
  ,FK_DIM_PERSON
  ,FK_DIM_GEOGRAFI
  ,KOMMUNE_NR
  ,BYDEL_NR
  ,STATSBORGERSKAP
  ,FODELAND
  ,SIVILSTATUS_KODE
  ,BEHANDLING_ID
  ,KLASSEKODE
  ,TSOTILBARN
  ,TSOTILBARN_ETTERBETALT
  ,FODSELS_AAR
  ,FODSELS_MND
  ,ALDER
  ,FK_DIM_KJONN
  ,KJONN
  ,AKTIVITET
  ,AKTIVITET_2
  ,ANTBARN
  ,ANTBU1
  ,ANTBU3
  ,ANTBU8
  ,ANTBU10
  ,ANTBU18
  ,localtimestamp AS lastet_dato
from ts_mottaker_data

{% if is_incremental() %}

where periode >= (select max(periode) from {{ this }})

{% endif %}


