{#-- Per-link interconnector flow and limits, joined to the hand-maintained link dimension.

     MWFLOW is the dispatch target, METEREDMWFLOW what actually flowed. EXPORTLIMIT /
     IMPORTLIMIT are the binding transfer limits for that interval -- the numbers that had to
     be quoted from outside knowledge before this model existed. Direction is defined by
     dim_interconnector: positive MWFLOW is FromRegion -> ToRegion.

     Consistency check that falls out for free: DISPATCHIS carries 6 INTERCONNECTORRES rows
     per interval and dim_interconnector has 7 rows, of which PEC (EnergyConnect) is
     InService = false. They agree.

     HISTORY: intraday only, ~2-day rolling window -- see fct_interconnector_today. Use
     fct_interconnector_derived for the full history at region-pair grain. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert',
    unique_key=['date', 'time', 'InterconnectorID'],
    partition_by=['date'],
    schema='mart'
) }}

WITH flows AS (
  SELECT
    i.DATE AS date,
    CAST(strftime(i.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    i.INTERCONNECTORID AS InterconnectorID,
    MAX(i.MWFLOW)          AS MWFlow,
    MAX(i.METEREDMWFLOW)   AS MeteredMWFlow,
    MAX(i.MWLOSSES)        AS MWLosses,
    MAX(i.EXPORTLIMIT)     AS ExportLimit,
    MAX(i.IMPORTLIMIT)     AS ImportLimit,
    MAX(i.MARGINALVALUE)   AS MarginalValue,
    MAX(i.MARGINALLOSS)    AS MarginalLoss,
    MAX(i.VIOLATIONDEGREE) AS ViolationDegree,
    MAX(i.FCASEXPORTLIMIT) AS FcasExportLimit,
    MAX(i.FCASIMPORTLIMIT) AS FcasImportLimit
  FROM {{ ref('fct_interconnector_today') }} i
  WHERE i.INTERVENTION = 0
  {%- if is_incremental() %}
    AND i.DATE >= (SELECT COALESCE(MAX(date), CAST('1900-01-01' AS DATE)) FROM {{ this }})
  {%- endif %}
  GROUP BY ALL
)

SELECT
  f.date,
  f.time,
  f.InterconnectorID,
  CAST(f.date AS VARCHAR) AS DateKey,
  d.Name,
  d.FromRegion,
  d.ToRegion,
  d.AcDc,
  d.InService,
  -- DOUBLE, never DECIMAL(p,s): see fct_region.
  CAST(f.MWFlow          AS DOUBLE) AS MWFlow,
  CAST(f.MeteredMWFlow   AS DOUBLE) AS MeteredMWFlow,
  CAST(f.MWLosses        AS DOUBLE) AS MWLosses,
  CAST(f.ExportLimit     AS DOUBLE) AS ExportLimit,
  CAST(f.ImportLimit     AS DOUBLE) AS ImportLimit,
  -- How close the link ran to its limit this interval. 0 means it bound.
  CAST(f.ExportLimit - f.MWFlow AS DOUBLE) AS ExportHeadroomMW,
  CAST(f.MarginalValue   AS DOUBLE) AS MarginalValue,
  CAST(f.MarginalLoss    AS DOUBLE) AS MarginalLoss,
  CAST(f.ViolationDegree AS DOUBLE) AS ViolationDegree,
  CAST(f.FcasExportLimit AS DOUBLE) AS FcasExportLimit,
  CAST(f.FcasImportLimit AS DOUBLE) AS FcasImportLimit
FROM flows f
LEFT JOIN {{ ref('dim_interconnector') }} d
  ON f.InterconnectorID = d.InterconnectorID
