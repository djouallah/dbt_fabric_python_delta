-- Generation rolled up to unit x date: the PRODUCED edge the graph was missing.
--
-- Same reasoning as agg_region_daily: fct_summary's DUID x 5-minute grain (~140M rows)
-- cannot sit in a graph that fully re-ingests on every save, but ~500 DUIDs x each
-- archived day is ~100k rows -- small enough to hang one UnitDay node per unit per day
-- without wrecking the traversals that touch Unit.
--
-- schema='mart' is explicit because the marts: block in dbt_project.yml defaults to
-- landing, and the ontology binds tables in mart.
{{ config(
    materialized='table',
    schema='mart'
) }}

SELECT
  -- Ontology entity keys may only be String or Integer, so the composite grain is carried
  -- as a string key; date stays available as its own column.
  f.DUID || '|' || CAST(f.date AS VARCHAR) AS UnitDayKey,
  f.DUID,
  f.date,
  -- GQL has no date functions or literals yet, but ISO-8601 date strings order
  -- lexicographically, so a String copy of the date makes graph-side range filters
  -- possible: WHERE ud.UnitDayDateKey >= '2026-08-03' AND ud.UnitDayDateKey <= '2026-08-09'.
  CAST(f.date AS VARCHAR) AS DateKey,
  -- DOUBLE, not DECIMAL(p, s). The ontology maps a parameterised `decimal(p, s)` to a
  -- STRING property while bare `decimal`/`double` map to Double -- so a DECIMAL(18,2)
  -- column bound to a Double property silently ingests as NULL.
  -- Net of charging: a battery's day can legitimately be negative.
  ROUND(SUM(f.mw) / 12.0, 2)::DOUBLE AS GenerationMWh,  -- 5-min data, 12 per hour
  ROUND(MAX(f.mw), 2)::DOUBLE AS PeakMW,
  ROUND(AVG(f.mw), 2)::DOUBLE AS AvgMW,
  -- 288 on a complete day. Carried so a partial day (today, or a gap in the archive) is
  -- visible rather than silently understating GenerationMWh.
  COUNT(*) AS IntervalCount
FROM {{ ref('fct_summary') }} f
JOIN {{ ref('dim_duid') }} d ON f.DUID = d.DUID
WHERE d.Region <> 'WA1'
GROUP BY f.DUID, f.date
