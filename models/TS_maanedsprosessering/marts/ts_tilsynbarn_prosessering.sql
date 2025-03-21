with ts as (
    select barn.*
          ,mottaker.ANTBARN
          ,mottaker.ANTBU1
          ,mottaker.ANTBU3
          ,mottaker.ANTBU8
          ,mottaker.ANTBU10
          ,mottaker.ANTBU18
          ,mottaker.YBARN
    from {{ source('fak_ts', 'ts_barn') }} barn
    join {{ source('fak_ts', 'ts_mottaker_barn') }} mottaker
    on barn.periode = mottaker.periode
    and barn.fk_person1 = mottaker.fk_person1
    where barn.klassekode in ('TSTBASISP2-OP','TSTBASISP3-OP') --Tilsynbarn
    and barn.periode = {{ var ('periode')}}
)

,
ts_mottaker_data as (
  SELECT
    {{ var ('periode')}} PERIODE,
    ts.FK_PERSON1,
    ts.FK_DIM_PERSON,
    dim_geografi.pk_dim_geografi FK_DIM_GEOGRAFI,
    DIM_PERSON.BOSTED_KOMMUNE_NR KOMMUNE_NR,
    DIM_GEOGRAFI.BYDEL_NR,
    DIM_PERSON.STATSBORGERSKAP,
    DIM_PERSON.FODELAND,
    DIM_PERSON.SIVILSTATUS_KODE,
    max(ts.HENVISNING) BEHANDLING_ID,
    ts.klassekode,
    SUM(CASE WHEN to_char(ts.DATO_UTBET_FOM,'YYYYMM')= {{ var ('periode')}} THEN ts.BELOP ELSE 0 END)  TSOTILBARN,
    SUM(CASE WHEN to_char(ts.DATO_UTBET_FOM,'YYYYMM')< {{ var ('periode')}} THEN ts.BELOP ELSE 0 END) TSOTILBARN_ETTERBETALT,
    to_char(DIM_PERSON.FODT_DATO,'YYYY') FODSELS_AAR,
    to_char(DIM_PERSON.FODT_DATO,'MM') FODSELS_MND,
    --floor(months_between(LAST_DAY(to_date({{ var ('periode')}}, 'YYYYMM')), dim_person.fodt_dato)/12) ALDER,
    floor(months_between(ts.siste_dato_i_perioden, dim_person_barn.fodt_dato)/12) ALDER,
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode KJONN,
    ts.ANTBARN,
    ts.ANTBU1,
    ts.ANTBU3,
    ts.ANTBU8,
    ts.ANTBU10,
    ts.ANTBU18,
    ts.YBARN
    from ts

    JOIN {{ source ('dt_person', 'dim_person') }} dim_person
    on ts.fk_dim_person = dim_person.pk_dim_person
    and dim_person.k67_flagg = 0 --Ikke ta med kode67 for aktuell statistikkperiode

    JOIN {{ source ('dt_kodeverk', 'dim_geografi') }} dim_geografi
    on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
    
    JOIN {{ source ('dt_kodeverk', 'dim_kjonn') }} dim_kjonn
    on dim_person.fk_dim_kjonn = dim_kjonn.pk_dim_kjonn

    GROUP BY
     ts.FK_PERSON1,
    ts.FK_DIM_PERSON,
    dim_geografi.pk_dim_geografi,
    DIM_PERSON.BOSTED_KOMMUNE_NR,
    DIM_GEOGRAFI.BYDEL_NR,
    DIM_PERSON.STATSBORGERSKAP,
    DIM_PERSON.FODELAND,
    DIM_PERSON.SIVILSTATUS_KODE,
    ts.klassekode,
    DIM_PERSON.FODT_DATO,
    dim_person.fk_dim_kjonn,
    dim_kjonn.kjonn_kode, 
    ts.ANTBARN,
    ts.ANTBU1,
    ts.ANTBU3,
    ts.ANTBU8,
    ts.ANTBU10,
    ts.ANTBU18,
    ts.YBARN
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
  ,ANTBARN
  ,ANTBU1
  ,ANTBU3
  ,ANTBU8
  ,ANTBU10
  ,ANTBU18
  ,localtimestamp AS lastet_dato
  ,YBARN
from ts_mottaker_data
