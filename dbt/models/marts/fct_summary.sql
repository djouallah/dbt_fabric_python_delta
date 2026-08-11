-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Overwrite-vs-append is decided by the RUNNER, not the model: `full_refresh` is fixed at
     parse time (execute=False) and can't be toggled from a run-time probe, so the old
     has_new_daily/config(full_refresh=...) trick was always pinned to overwrite. Instead the
     runner (notebook + CI) checks the `check_new_daily` run-operation and reruns this model
     with `--full-refresh` only when a new daily file landed (-> is_incremental() false ->
     full rebuild from daily, duckrun overwrites); a plain run appends today's intraday
     (-> is_incremental() true). --#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{% if is_incremental() %}

-- Append intraday: today's rows after the last cutoff baked into the table.
WITH max_cutoff AS (
  SELECT MAX(cutoff) as cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.DATE as date,
    s.SETTLEMENTDATE,
    s.DUID,
    d.Region AS RegionID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
)

SELECT
  date,
  -- HHMM, not minutes past midnight: 0, 5, ... 1255, 1300, ... 2355. 288 distinct values.
  CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT) AS time,
  DUID,
  -- Carried so region filtering doesn't need a dim_duid re-join. Both branches must set it.
  RegionID,
  -- DOUBLE, not DECIMAL(18,4): the ontology's v3 TimeSeries binding maps parameterised
  -- decimals to String properties, which ingest as NULL against a Double property.
  CAST(mw AS DOUBLE) AS mw,
  CAST(price AS DOUBLE) AS price,
  -- ISO date as a string: ontology entity KEY parts may only be String/Integer, so the
  -- v4 Observation entity keys on [DUID, DateKey, time] -- DATE itself is banned there.
  CAST(date AS VARCHAR) AS DateKey,
  CAST(MAX(SETTLEMENTDATE) OVER () AS TIMESTAMPTZ) AS cutoff
FROM incremental_data

{% else %}

-- Full rebuild (runs under --full-refresh -> overwrite): authoritative daily + today's
-- intraday after the daily cutoff. The cutoff column is the watermark the append path reads.
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    d.Region AS RegionID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL

  UNION ALL

  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    d.Region AS RegionID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  RegionID,
  -- DOUBLE + RegionID: see the incremental branch comment; both branches stay in lockstep.
  CAST(mw AS DOUBLE) AS mw,
  CAST(price AS DOUBLE) AS price,
  CAST(date AS VARCHAR) AS DateKey,
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary
ORDER BY date

{% endif %}
