with ts_mottaker_barn_data as (
  SELECT PERIODE, FK_PERSON1, COUNT(DISTINCT FK_PERSON1_BARN) ANTBARN,
  SUM(BU1) ANTBU1,
  SUM(BU3) ANTBU3,
  SUM(BU8) ANTBU8,
  SUM(BU10) ANTBU10,
  SUM(BU18) ANTBU18,
  MIN(ALDER_BARN) YBARN
  FROM {{ ref('ts_barn') }}
  GROUP BY PERIODE,FK_PERSON1
)

select * from ts_mottaker_barn_data
where periode = {{ var('periode') }}