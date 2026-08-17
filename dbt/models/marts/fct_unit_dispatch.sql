{#-- Unit dispatch at 5-minute grain, wide. `fct_scada` lands the whole DUNIT record (49
     columns); fct_summary kept one of them (INITIALMW). The three that matter most and were
     being thrown away:
       TOTALCLEARED  -- what the unit was DISPATCHED to, vs INITIALMW which is where it was
       AVAILABILITY  -- what the unit OFFERED. Spare headroom is AVAILABILITY - INITIALMW,
                        a real number, instead of being inferred from observed peaks.
       RAMPUPRATE / RAMPDOWNRATE -- how fast it can actually move.

     Deliberately does NOT filter INITIALMW <> 0, unlike fct_summary. A unit that offered
     capacity and was not dispatched is the most useful row in this table -- dropping it is
     exactly what makes headroom un-computable. Cost: ~3x fct_summary's row count. If the
     first full build hurts, the fallback is WHERE AvailableMW > 0 OR INITIALMW <> 0, which
     keeps the analytically interesting rows and drops the genuinely idle ones.

     Daily-authoritative only: there is no intraday leg, because the intraday source
     (DISPATCHSCADA) carries only SCADAVALUE -- none of these columns exist in it. This table
     therefore lags by up to a day BY DESIGN; fct_summary remains the live view. --#}
{{ config(
    materialized='table',
    schema='mart'
) }}

WITH unit_dispatch AS (
  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS TimeHHMM,
    s.DUID,
    d.RegionID,
    -- MAX collapses duplicates from multiple runnos/files for one interval.
    MAX(s.INITIALMW)       AS InitialMW,
    MAX(s.TOTALCLEARED)    AS TotalClearedMW,
    MAX(s.AVAILABILITY)    AS AvailableMW,
    MAX(s.RAMPUPRATE)      AS RampUpRate,
    MAX(s.RAMPDOWNRATE)    AS RampDownRate,
    MAX(s.DISPATCHMODE)    AS DispatchMode,
    MAX(s.AGCSTATUS)       AS AgcStatus,
    MAX(s.MARGINALVALUE)   AS MarginalValue,
    MAX(s.VIOLATIONDEGREE) AS ViolationDegree,
    MAX(s.RAISEREG)        AS RaiseReg,
    MAX(s.LOWERREG)        AS LowerReg,
    MAX(s.RAISE6SEC)       AS Raise6Sec,
    MAX(s.RAISE60SEC)      AS Raise60Sec,
    MAX(s.RAISE5MIN)       AS Raise5Min,
    MAX(s.LOWER6SEC)       AS Lower6Sec,
    MAX(s.LOWER60SEC)      AS Lower60Sec,
    MAX(s.LOWER5MIN)       AS Lower5Min,
    MAX(s.month_key)       AS month_key
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  WHERE s.INTERVENTION = 0
  GROUP BY ALL
)

SELECT
  date,
  TimeHHMM,
  DUID,
  RegionID,
  CAST(date AS VARCHAR) AS DateKey,
  -- DOUBLE, never DECIMAL(p,s): see fct_region.
  CAST(InitialMW       AS DOUBLE) AS InitialMW,
  CAST(TotalClearedMW  AS DOUBLE) AS TotalClearedMW,
  CAST(AvailableMW     AS DOUBLE) AS AvailableMW,
  -- Offered but not dispatched. The whole reason this model exists.
  CAST(AvailableMW - InitialMW AS DOUBLE) AS HeadroomMW,
  CAST(RampUpRate      AS DOUBLE) AS RampUpRate,
  CAST(RampDownRate    AS DOUBLE) AS RampDownRate,
  CAST(DispatchMode    AS DOUBLE) AS DispatchMode,
  CAST(AgcStatus       AS DOUBLE) AS AgcStatus,
  CAST(MarginalValue   AS DOUBLE) AS MarginalValue,
  CAST(ViolationDegree AS DOUBLE) AS ViolationDegree,
  CAST(RaiseReg        AS DOUBLE) AS RaiseReg,
  CAST(LowerReg        AS DOUBLE) AS LowerReg,
  CAST(Raise6Sec       AS DOUBLE) AS Raise6Sec,
  CAST(Raise60Sec      AS DOUBLE) AS Raise60Sec,
  CAST(Raise5Min       AS DOUBLE) AS Raise5Min,
  CAST(Lower6Sec       AS DOUBLE) AS Lower6Sec,
  CAST(Lower60Sec      AS DOUBLE) AS Lower60Sec,
  CAST(Lower5Min       AS DOUBLE) AS Lower5Min,
  month_key
FROM unit_dispatch
