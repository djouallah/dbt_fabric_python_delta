-- Generation rolled up to region x date: the grain the graph can actually hold.
--
-- fct_summary is ~140M rows at DUID x 5-minute grain. Fabric re-ingests the whole graph
-- on every save, and hanging ~190k leaf observations off each Unit node would wreck any
-- traversal that touches it. fct_summary also has no single timestamp column (date DATE +
-- time INT HHMM), so an ontology time-series binding could only key on `date` -- which
-- collapses all 288 intervals onto one timestamp anyway. If daily is the only grain the
-- binding can express, aggregate to daily: same answer, ~15k rows instead of 140M.
--
-- schema='mart' is explicit because the marts: block in dbt_project.yml defaults to
-- landing, and the ontology binds tables in mart.
{{ config(
    materialized='table',
    schema='mart'
) }}

WITH interval_totals AS (
  SELECT
    d.Region,
    f.date,
    f.time,
    SUM(f.mw)   AS region_mw,
    -- price is the regional RRP repeated against every DUID in the region, so averaging
    -- across DUIDs within an interval returns the RRP itself.
    AVG(f.price) AS price
  FROM {{ ref('fct_summary') }} f
  JOIN {{ ref('dim_duid') }} d ON f.DUID = d.DUID
  WHERE d.Region <> 'WA1'
  GROUP BY d.Region, f.date, f.time
)

SELECT
  -- Ontology entity keys may only be String or Integer, so the composite grain is carried
  -- as a string key; date stays available as its own column.
  Region || '|' || CAST(date AS VARCHAR) AS RegionDayKey,
  Region,
  date,
  -- DOUBLE, not DECIMAL(p, s). The ontology maps a parameterised `decimal(p, s)` to a
  -- STRING property while bare `decimal`/`double` map to Double -- so a DECIMAL(18,2)
  -- column bound to a Double property silently ingests as NULL.
  ROUND(SUM(region_mw) / 12.0, 2)::DOUBLE AS GenerationMWh,  -- 5-min data, 12 per hour
  -- A regional peak is the max over intervals of the SUMMED regional MW, not the max of
  -- any single DUID -- hence the two-step aggregation.
  ROUND(MAX(region_mw), 2)::DOUBLE AS PeakMW,
  ROUND(AVG(region_mw), 2)::DOUBLE AS AvgMW,
  ROUND(AVG(price), 2)::DOUBLE     AS AvgPrice,
  -- 288 on a complete day. Carried so a partial day (today, or a gap in the archive) is
  -- visible rather than silently understating GenerationMWh.
  COUNT(*) AS IntervalCount
FROM interval_totals
GROUP BY Region, date
