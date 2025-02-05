with ts_mottaker_data as (
  SELECT
    {{ var ('periode')}} PERIODE,
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
    SUM(CASE WHEN to_char(DATO_UTBET_FOM,'YYYYMM')= {{ var ('periode')}} THEN UR.BELOP ELSE 0 END)  TSOTILBARN,
    SUM(CASE WHEN to_char(DATO_UTBET_FOM,'YYYYMM')< {{ var ('periode')}} THEN UR.BELOP ELSE 0 END) TSOTILBARN_ETTERBETALT,
    to_char(DIM_PERSON.FODT_DATO,'YYYY') FODSELS_AAR,
    to_char(DIM_PERSON.FODT_DATO,'MM') FODSELS_MND,
    floor(months_between(LAST_DAY(to_date({{ var ('periode')}}, 'YYYYMM')), dim_person.fodt_dato)/12) ALDER,
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode KJONN,
    max(AKT.AKTIVITET) AKTIVITET,
    max(AKT.AKTIVITET_2) AKTIVITET_2,
    BARN.ANTBARN,
    BARN.ANTBU1,
    BARN.ANTBU3,
    BARN.ANTBU8,
    BARN.ANTBU10,
    BARN.ANTBU18,
    BARN.YBARN
    from {{ source ('fam_ef', 'fam_ts_ur_utbetaling') }} UR

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

    JOIN {{ source ('dt_person_arena', 'DIM_PERSON') }} dim_person
    on ur.fk_dim_person = dim_person.pk_dim_person
    and dim_person.k67_flagg = 0

    JOIN {{ source ('arena_stonad', 'dim_geografi') }} dim_geografi
    on dim_person.fk_dim_geografi_bosted=dim_geografi.pk_dim_geografi
    
    JOIN {{ source ('arena_stonad', 'dim_kjonn') }} dim_kjonn
    on dim_person.fk_dim_kjonn=dim_kjonn.pk_dim_kjonn

    LEFT OUTER JOIN {{ ref ('ts_mottaker_barn') }} BARN ON
    UR.FK_PERSON1=BARN.FK_PERSON1
    AND {{ var ('periode')}} = BARN.PERIODE

    where to_char(UR.POSTERINGSDATO,'YYYYMM') = {{ var ('periode')}}
    AND UR.KLASSEKODE IN ('TSTBASISP2-OP','TSTBASISP3-OP')
    
    GROUP BY
     UR.FK_PERSON1,
    UR.FK_DIM_PERSON,
    dim_geografi.pk_dim_geografi,
    DIM_PERSON.BOSTED_KOMMUNE_NR,
    DIM_GEOGRAFI.BYDEL_NR,
    DIM_PERSON.STATSBORGERSKAP,
    DIM_PERSON.FODELAND,
    DIM_PERSON.SIVILSTATUS_KODE,
    ur.klassekode,
    DIM_PERSON.FODT_DATO,
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode, 
    --AKT.AKTIVITET,
    --AKT.AKTIVITET_2,
    BARN.ANTBARN,
    BARN.ANTBU1,
    BARN.ANTBU3,
    BARN.ANTBU8,
    BARN.ANTBU10,
    BARN.ANTBU18,
    BARn.YBARN
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
  ,YBARN
from ts_mottaker_data
