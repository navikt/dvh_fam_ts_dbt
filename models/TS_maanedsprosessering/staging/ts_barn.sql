with ts_barn_data as (
  SELECT
    DISTINCT UR.FK_PERSON1,
    {{ var('periode') }} PERIODE,
    BARN.FK_PERSON1 FK_PERSON1_BARN,
    to_char(DIM_PERSON_BARN.FODT_DATO,'YYYY') FODT_AAR_BARN,
    to_char(DIM_PERSON_BARN.FODT_DATO,'MM') FODT_MND_BARN,
    floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) ALDER_BARN,
    CASE WHEN floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 1 THEN 1 ELSE 0 END BU1,
    CASE WHEN floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 3 THEN 1 ELSE 0 END BU3,
    CASE WHEN floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 8 THEN 1 ELSE 0 END BU8,
    CASE WHEN floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 10 THEN 1 ELSE 0 END BU10,
    CASE WHEN floor(months_between(tid.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) < 18 THEN 1 ELSE 0 END BU18
    --
    from {{ source ( 'fam_ef','fam_ts_ur_utbetaling' )}} UR

    JOIN {{ source ( 'dt_kodeverk','dim_tid' )}} TID
    ON TID.dato = UR.posteringsdato
    and tid.gyldig_flagg = 1
    and tid.dim_nivaa = 1

    JOIN {{ source ( 'fam_ef','FAM_TS_BARN' )}} BARN ON
    ur.henvisning = BARN.EKSTERN_BEHANDLING_ID

    LEFT OUTER JOIN {{ source ( 'dt_person','dim_person' )}} DIM_PERSON_BARN
    ON DIM_PERSON_BARN.FK_PERSON1 = BARN.FK_PERSON1
    AND TID.siste_dato_i_perioden BETWEEN DIM_PERSON_BARN.GYLDIG_FRA_DATO AND DIM_PERSON_BARN.GYLDIG_TIL_DATO

    where to_char(ur.posteringsdato, 'yyyymm') = {{ var('periode') }}
)

select * from ts_barn_data