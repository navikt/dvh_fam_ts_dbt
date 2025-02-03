{{
    config(
        materialized = 'incremental',
        unique_key = 'periode',
        incremental_strategy='delete+insert',
    )
}}


with mottaker_data as (
  SELECT
    {{ var ('periode')}} periode
    ,nvl(ts.FK_PERSON1, lm.fk_person1) FK_PERSON1
    ,nvl(ts.FK_DIM_PERSON, lm.FK_DIM_PERSON) FK_DIM_PERSON
    ,nvl(ts.FK_DIM_GEOGRAFI, lm.FK_DIM_GEOGRAFI) FK_DIM_GEOGRAFI
    ,nvl(ts.KOMMUNE_NR, lm.KOMMUNE_NR) KOMMUNE_NR
    ,nvl(ts.BYDEL_NR, lm.BYDEL_NR) BYDEL_NR
    ,nvl(ts.STATSBORGERSKAP, lm.STATSBORGERSKAP) STATSBORGERSKAP
    ,nvl(ts.FODELAND, lm.FODELAND) FODELAND
    ,nvl(ts.SIVILSTATUS_KODE, lm.SIVILSTATUS_KODE) SIVILSTATUS_KODE
    ,nvl(ts.BEHANDLING_ID, lm.BEHANDLING_ID) BEHANDLING_ID
    ,ts.KLASSEKODE ts_klassekode
    ,lm.KLASSEKODE lm_klassekode
    ,ts.TSOTILBARN
    ,ts.TSOTILBARN_ETTERBETALT
    ,lm.TSLM
    ,lm.TSLM_ETTERBETALT
    ,nvl(ts.FODSELS_AAR, lm.FODSELS_AAR) FODSELS_AAR
    ,nvl(ts.FODSELS_MND, lm.FODSELS_MND) FODSELS_MND
    ,nvl(ts.ALDER, lm.ALDER) ALDER
    ,nvl(ts.FK_DIM_KJONN, lm.FK_DIM_KJONN) FK_DIM_KJONN
    ,nvl(ts.KJONN, lm.KJONN) KJONN
    ,nvl(ts.AKTIVITET, lm.AKTIVITET) AKTIVITET
    ,nvl(ts.AKTIVITET_2, lm.AKTIVITET_2) AKTIVITET_2
    ,ts.ANTBARN
    ,ts.ANTBU1
    ,ts.ANTBU3
    ,ts.ANTBU8
    ,ts.ANTBU10
    ,ts.ANTBU18
    ,localtimestamp AS lastet_dato 
  from 
    {{ ref('ts_prosessering') }} ts

  full outer join {{ ref('lm_prosessering') }} lm
  on ts.FK_PERSON1 = lm.FK_PERSON1

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
    ,ts_klassekode
    ,lm_klassekode
    ,TSOTILBARN
    ,TSOTILBARN_ETTERBETALT
    ,TSLM
    ,TSLM_ETTERBETALT
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
    ,lastet_dato 
  from mottaker_data

  {% if is_incremental() %}

  where periode >= (select max(periode) from {{ this }})

  {% endif %}






