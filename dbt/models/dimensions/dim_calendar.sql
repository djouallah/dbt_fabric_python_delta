{{ config(
    materialized='table'
) }}

-- Fixed calendar dimension, rebuilt every run. It is a generate_series over ~3,200 days —
-- cheaper to rebuild than to reason about.
SELECT
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
