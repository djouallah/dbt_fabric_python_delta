{{ config(tags=['heavy']) }}
-- Summary should have at least as many distinct days as scada
-- Tagged heavy: some ingested daily files have all INTERVENTION=1 rows or
-- unmapped DUIDs and produce zero summary rows for that date, so this only
-- holds once the whole archive is loaded.
SELECT
  scada_days,
  summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
    (SELECT COUNT(DISTINCT date) FROM {{ ref('fct_summary') }}) as summary_days
)
WHERE scada_days > summary_days
