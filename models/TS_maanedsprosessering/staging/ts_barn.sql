with ts_barn_data as (
  SELECT
    DISTINCT UR.FK_PERSON1,
    to_char(tid.dato,'YYYYMM') PERIODE,
    BARN.FK_PERSON1 FK_PERSON1_BARN,
    to_char(DIM_PERSON_BARN.FODT_DATO,'YYYY') FODT_AAR_BARN,
    to_char(DIM_PERSON_BARN.FODT_DATO,'MM') FODT_MND_BARN,
    floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) ALDER_BARN,
    CASE WHEN floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) < 1 THEN 1 ELSE 0 END BU1,
    CASE WHEN floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) < 3 THEN 1 ELSE 0 END BU3,
    CASE WHEN floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) < 8 THEN 1 ELSE 0 END BU8,
    CASE WHEN floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) < 10 THEN 1 ELSE 0 END BU10,
    CASE WHEN floor(months_between(tid.dato, dim_person_barn.fodt_dato)/12) < 18 THEN 1 ELSE 0 END BU18
    --
    from {{ source ( 'ur','fak_ur_utbetaling' )}} UR

    JOIN {{ source ( 'kode_verk','dim_tid' )}} TID
    ON TID.PK_DIM_TID = UR.FK_DIM_TID_DATO_POSTERT_UR

    JOIN {{ source ( 'ur','DIM_KONTO_UR' )}} KONTO ON
    UR.FK_DIM_KONTO_UR = KONTO.PK_DIM_KONTO_UR

    JOIN {{ source ( 'fam_ef','FAM_TS_BARN' )}} BARN ON
    ur.henvisning=BARN.EKSTERN_BEHANDLING_ID

    LEFT OUTER JOIN {{ source ( 'dt_person_arena','DIM_PERSON' )}} DIM_PERSON_BARN ON
    DIM_PERSON_BARN.FK_PERSON1=BARN.FK_PERSON1 AND
    TID.DATO BETWEEN DIM_PERSON_BARN.GYLDIG_FRA_DATO AND DIM_PERSON_BARN.GYLDIG_TIL_DATO

    where konto.HOVEDKONTONR IN ('777') -- Tilleggsstønader
    and UR.FK_DIM_TID_DATO_POSTERT_UR >= 20240501
    AND UR.KLASSEKODE IN (--'TSTBASISP4-OP',
    'TSTBASISP2-OP','TSTBASISP3-OP'--,'TSTBASISP5-OP'
    )
    --and ur.henvisning in ('8','34','37')
    AND LENGTH(UR.HENVISNING)>1
)

select * from ts_barn_data
where periode = {{ var('periode') }}