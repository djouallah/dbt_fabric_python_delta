{#-- CONSTRAINT out of the DISPATCHIS report -- which network limits the dispatch engine
     actually applied, their RHS/LHS and marginal value. This is the "why" behind a flow
     stopping where it did.

     Same archive files as fct_price_today / fct_interconnector_today (source_type =
     'price_today'), different record filter. No new download.

     VOLUME: ~1,053 rows per 5-minute interval, so ~303k rows/day. Landing keeps all of them;
     the fct_constraint mart narrows to the ones that actually bound.

     HISTORY WARNING: same as fct_interconnector_today -- DISPATCHIS is a rolling ~2-day
     Current report and the PUBLIC_DAILY archive contains no CONSTRAINT record, so this table
     starts empty and only accumulates forward. --#}
{{ config(
    materialized='table',
    pre_hook="SET VARIABLE constraint_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'price_today'))"
) }}

WITH constraint_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('constraint_today_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I': 'VARCHAR',
      'DISPATCH': 'VARCHAR',
      'RECORD': 'VARCHAR',
      'xx': 'VARCHAR',
      'SETTLEMENTDATE': 'timestamp',
      'RUNNO': 'VARCHAR',
      'CONSTRAINTID': 'VARCHAR',
      'DISPATCHINTERVAL': 'VARCHAR',
      'INTERVENTION': 'VARCHAR',
      'RHS': 'VARCHAR',
      'MARGINALVALUE': 'VARCHAR',
      'VIOLATIONDEGREE': 'VARCHAR',
      'LASTCHANGED': 'VARCHAR',
      'DUID': 'VARCHAR',
      'GENCONID_EFFECTIVEDATE': 'VARCHAR',
      'GENCONID_VERSIONNO': 'VARCHAR',
      'LHS': 'VARCHAR'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND RECORD = 'CONSTRAINT'
)

SELECT
  CONSTRAINTID,
  DUID,
  CAST(RUNNO AS DOUBLE) AS RUNNO,
  CAST(DISPATCHINTERVAL AS DOUBLE) AS DISPATCHINTERVAL,
  CAST(INTERVENTION AS DOUBLE) AS INTERVENTION,
  CAST(RHS AS DOUBLE) AS RHS,
  CAST(LHS AS DOUBLE) AS LHS,
  CAST(MARGINALVALUE AS DOUBLE) AS MARGINALVALUE,
  CAST(VIOLATIONDEGREE AS DOUBLE) AS VIOLATIONDEGREE,
  GENCONID_EFFECTIVEDATE,
  CAST(GENCONID_VERSIONNO AS DOUBLE) AS GENCONID_VERSIONNO,
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR
FROM constraint_staging
