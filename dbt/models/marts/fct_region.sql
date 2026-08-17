{#-- Regional dispatch outcome, 5-minute grain -- the half of the AEMO data the pipeline has
     always landed and never exposed. `fct_price` is not a price table: it is AEMO's DREGION
     record, 126 columns, of which fct_summary kept exactly one (RRP). Demand, net interchange
     and available generation all live here.

     SIGN CONVENTION, the thing everyone gets backwards:
       NETINTERCHANGE is POSITIVE when the region is a net EXPORTER.
     Verified against the identity that must hold every interval:
       DISPATCHABLEGENERATION - DISPATCHABLELOAD - NETINTERCHANGE = TOTALDEMAND
     (QLD1, 2026-08-09: 7023 - 319 - 1086 = 5618.)

     Because QLD1, SA1 and TAS1 each have exactly ONE neighbour, their NETINTERCHANGE *is*
     the flow on the interconnector(s) to that neighbour -- see fct_interconnector_derived,
     which solves the whole network from this column alone.

     Reads the Delta table, not CSVs, so there is no archive pre_hook: the incremental filter
     is by date instead of by file. Small table -- ~288 intervals x 5 regions x history. --#}
{{ config(
    materialized='table',
    schema='mart'
) }}

WITH region_dispatch AS (
  SELECT
    p.DATE AS date,
    CAST(strftime(p.SETTLEMENTDATE, '%H%M') AS INT) AS TimeHHMM,
    p.REGIONID AS RegionID,
    -- MAX collapses the residual duplicates left by multiple runnos/files for one interval,
    -- the same dedup fct_summary relies on.
    MAX(p.RRP)                    AS Price,
    MAX(p.TOTALDEMAND)            AS TotalDemand,
    MAX(p.DEMANDFORECAST)         AS DemandForecast,
    MAX(p.DISPATCHABLEGENERATION) AS DispatchableGeneration,
    MAX(p.DISPATCHABLELOAD)       AS DispatchableLoad,
    MAX(p.NETINTERCHANGE)         AS NetInterchange,
    MAX(p.AVAILABLEGENERATION)    AS AvailableGeneration,
    MAX(p.AVAILABLELOAD)          AS AvailableLoad,
    MAX(p.INITIALSUPPLY)          AS InitialSupply,
    MAX(p.CLEAREDSUPPLY)          AS ClearedSupply,
    MAX(p.EXCESSGENERATION)       AS ExcessGeneration,
    MAX(p.APCFLAG)                AS ApcFlag,
    MAX(p.MARKETSUSPENDEDFLAG)    AS MarketSuspendedFlag,
    MAX(p.LORSURPLUS)             AS LorSurplus,
    MAX(p.LRCSURPLUS)             AS LrcSurplus,
    MAX(p.month_key)              AS month_key
  FROM {{ ref('fct_price') }} p
  WHERE p.INTERVENTION = 0
  GROUP BY ALL
)

SELECT
  date,
  TimeHHMM,
  RegionID,
  -- ISO date as a string: ontology entity KEY parts may only be String/Integer, so any
  -- future entity keyed on this table needs DateKey (YYYYMMDD int) rather than DATE.
  CAST(strftime(date, '%Y%m%d') AS INT) AS DateKey,
  -- DOUBLE, never DECIMAL(p,s): a parameterised decimal binds to a String ontology property
  -- and ingests as silent NULL against a Double property.
  CAST(Price                  AS DOUBLE) AS Price,
  CAST(TotalDemand            AS DOUBLE) AS TotalDemand,
  CAST(DemandForecast         AS DOUBLE) AS DemandForecast,
  CAST(DispatchableGeneration AS DOUBLE) AS DispatchableGeneration,
  CAST(DispatchableLoad       AS DOUBLE) AS DispatchableLoad,
  CAST(NetInterchange         AS DOUBLE) AS NetInterchange,
  CAST(AvailableGeneration    AS DOUBLE) AS AvailableGeneration,
  CAST(AvailableLoad          AS DOUBLE) AS AvailableLoad,
  CAST(InitialSupply          AS DOUBLE) AS InitialSupply,
  CAST(ClearedSupply          AS DOUBLE) AS ClearedSupply,
  CAST(ExcessGeneration       AS DOUBLE) AS ExcessGeneration,
  CAST(ApcFlag                AS DOUBLE) AS ApcFlag,
  CAST(MarketSuspendedFlag    AS DOUBLE) AS MarketSuspendedFlag,
  CAST(LorSurplus             AS DOUBLE) AS LorSurplus,
  CAST(LrcSurplus             AS DOUBLE) AS LrcSurplus,
  month_key
FROM region_dispatch
