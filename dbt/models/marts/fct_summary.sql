-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- The analytical spine: one row per unit per 5-minute interval, unit output joined to the
     regional price it faced. Rebuilt in full every run from the authoritative daily archive
     plus today's intraday tail after the daily cutoff. --#}
{{ config(
    materialized='table',
    schema='mart'
) }}

WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as TimeHHMM,
    s.DUID,
    d.RegionID,
    MAX(s.INITIALMW) as MW,
    MAX(p.RRP) as Price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.RegionID = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL

  UNION ALL

  -- Today's intraday tail: only the intervals published after the daily archive's cutoff, so
  -- the two sources never double-count the same interval.
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as TimeHHMM,
    s.DUID,
    d.RegionID,
    MAX(s.INITIALMW) as MW,
    MAX(p.RRP) as Price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.RegionID = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  date,
  -- HHMM, not minutes past midnight: 0, 5, ... 1255, 1300, ... 2355. 288 distinct values.
  TimeHHMM,
  DUID,
  -- Carried so region filtering doesn't need a dim_duid re-join.
  RegionID,
  -- DOUBLE, not DECIMAL(18,4): the ontology maps a parameterised decimal to a String
  -- property, which then ingests as NULL against a Double property -- silently.
  CAST(MW AS DOUBLE) AS MW,
  CAST(Price AS DOUBLE) AS Price,
  -- YYYYMMDD integer: ontology entity KEY parts may only be String or Integer, so the
  -- Observation entity keys on [DUID, DateKey, TimeHHMM] -- DATE itself is banned there.
  -- Integer, not an ISO string: it compares and range-filters in GQL without quoting,
  -- and sorts identically. `date` stays a real DATE for Power BI's calendar join.
  CAST(strftime(date, '%Y%m%d') AS INT) AS DateKey
FROM daily_summary
ORDER BY date
