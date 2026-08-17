{#-- The network constraints that actually BOUND, per dispatch interval.

     A constraint is a limit equation the dispatch engine must respect. Roughly 1,053 are
     evaluated every 5 minutes and almost all are slack -- LHS well under RHS, marginal value
     zero, no influence on the outcome. Those rows are noise at ~303k/day.

     This model keeps only the ones that did something: MARGINALVALUE <> 0 (the constraint was
     binding, and the value is the $/MW shadow Price of relaxing it) or VIOLATIONDEGREE <> 0
     (it could not even be met). Landing keeps the full set if the slack rows are ever needed.

     MarginalValue is the interesting column: it is exactly how much cheaper dispatch would
     have been with one more MW of that limit -- which is what makes "why was the flow capped
     here" answerable.

     HISTORY: intraday only, ~2-day rolling window -- see fct_constraint_today. --#}
{{ config(
    materialized='table',
    schema='mart'
) }}

SELECT
  c.DATE AS date,
  CAST(strftime(c.SETTLEMENTDATE, '%H%M') AS INT) AS TimeHHMM,
  c.CONSTRAINTID AS ConstraintID,
  CAST(strftime(c.DATE, '%Y%m%d') AS INT) AS DateKey,
  c.DUID,
  -- DOUBLE, never DECIMAL(p,s): see fct_region.
  CAST(MAX(c.RHS)             AS DOUBLE) AS RHS,
  CAST(MAX(c.LHS)             AS DOUBLE) AS LHS,
  CAST(MAX(c.MARGINALVALUE)   AS DOUBLE) AS MarginalValue,
  CAST(MAX(c.VIOLATIONDEGREE) AS DOUBLE) AS ViolationDegree,
  CAST(MAX(c.GENCONID_VERSIONNO) AS DOUBLE) AS GenConIdVersionNo
FROM {{ ref('fct_constraint_today') }} c
WHERE c.INTERVENTION = 0
  -- Binding or violated only. Slack constraints are ~99% of the rows and carry no signal.
  AND (c.MARGINALVALUE <> 0 OR c.VIOLATIONDEGREE <> 0)
GROUP BY ALL
