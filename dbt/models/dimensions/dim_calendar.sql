{{ config(
    materialized='table'
) }}

-- Fixed calendar dimension, rebuilt every run. It is a generate_series over ~3,200 days —
-- cheaper to rebuild than to reason about.
--
-- DateKey (YYYYMMDD int) is the KEY every fact joins on; `date` is the real DATE that Power
-- BI's date intelligence needs. The facts carry only DateKey, so the one place a calendar
-- date exists is here.
SELECT
  CAST(strftime(date, '%Y%m%d') AS INT) as DateKey,
  CAST(date AS DATE) as date,
  CAST(EXTRACT(year FROM date) AS INT) as year,
  CAST(EXTRACT(month FROM date) AS INT) as month
FROM (
  SELECT unnest(generate_series(
    CAST('2018-04-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) as date
)
