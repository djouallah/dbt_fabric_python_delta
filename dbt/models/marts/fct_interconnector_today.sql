{#-- INTERCONNECTORRES out of the DISPATCHIS report -- the real per-link flows AND limits.

     These are the SAME archive files fct_price_today already reads (source_type =
     'price_today'); that model's `WHERE PRICE = 'PRICE'` filter was throwing six other record
     types away. So this costs no new download and no change to stg_csv_archive_log.py -- the
     bytes are already in OneLake. Record widths differ per type, which is why each record
     gets its own column list and its own `RECORD = '...'` filter.

     HISTORY WARNING: DISPATCHIS is a Current report on a rolling ~500-file window (about two
     days). The PUBLIC_DAILY archive does NOT contain INTERCONNECTORRES -- it holds only
     DUNIT, DREGION, DISPATCH.CASESOLUTION and DISPATCH.REGIONFCASREQUIREMENT. So this table
     starts empty and accumulates forward; it will never have back-history. That is AEMO's
     publishing, not a bug here. For full-history flows see fct_interconnector_derived, which
     solves the network from fct_region.NetInterchange. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by=['DATE'],
    pre_hook="SET VARIABLE interconnector_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'price_today'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} LIMIT {{ env_var('process_limit', '1000') }}))"
) }}

{#-- Skip the file read entirely when no new price_today files have arrived: otherwise the
     pre_hook's COALESCE(..., ['']) sentinel makes read_csv('') run against an empty path. --#}
{%- set check_files_query -%}
SELECT COUNT(*) as cnt FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'price_today'
{%- if is_incremental() %}
AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH == 'run' -%}
  {%- set files_result = run_query(check_files_query) -%}
  {%- set has_files = files_result and files_result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_files = true -%}
{%- endif -%}

{% if has_files %}
WITH interconnector_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('interconnector_today_paths'),
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
      'INTERCONNECTORID': 'VARCHAR',
      'DISPATCHINTERVAL': 'VARCHAR',
      'INTERVENTION': 'VARCHAR',
      'METEREDMWFLOW': 'VARCHAR',
      'MWFLOW': 'VARCHAR',
      'MWLOSSES': 'VARCHAR',
      'MARGINALVALUE': 'VARCHAR',
      'VIOLATIONDEGREE': 'VARCHAR',
      'LASTCHANGED': 'VARCHAR',
      'EXPORTLIMIT': 'VARCHAR',
      'IMPORTLIMIT': 'VARCHAR',
      'MARGINALLOSS': 'VARCHAR',
      'EXPORTGENCONID': 'VARCHAR',
      'IMPORTGENCONID': 'VARCHAR',
      'FCASEXPORTLIMIT': 'VARCHAR',
      'FCASIMPORTLIMIT': 'VARCHAR',
      'LOCAL_PRICE_ADJUSTMENT_EXPORT': 'VARCHAR',
      'LOCALLY_CONSTRAINED_EXPORT': 'VARCHAR',
      'LOCAL_PRICE_ADJUSTMENT_IMPORT': 'VARCHAR',
      'LOCALLY_CONSTRAINED_IMPORT': 'VARCHAR'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND RECORD = 'INTERCONNECTORRES'
)

SELECT
  INTERCONNECTORID,
  CAST(RUNNO AS DOUBLE) AS RUNNO,
  CAST(DISPATCHINTERVAL AS DOUBLE) AS DISPATCHINTERVAL,
  CAST(INTERVENTION AS DOUBLE) AS INTERVENTION,
  CAST(METEREDMWFLOW AS DOUBLE) AS METEREDMWFLOW,
  CAST(MWFLOW AS DOUBLE) AS MWFLOW,
  CAST(MWLOSSES AS DOUBLE) AS MWLOSSES,
  CAST(MARGINALVALUE AS DOUBLE) AS MARGINALVALUE,
  CAST(VIOLATIONDEGREE AS DOUBLE) AS VIOLATIONDEGREE,
  CAST(EXPORTLIMIT AS DOUBLE) AS EXPORTLIMIT,
  CAST(IMPORTLIMIT AS DOUBLE) AS IMPORTLIMIT,
  CAST(MARGINALLOSS AS DOUBLE) AS MARGINALLOSS,
  EXPORTGENCONID,
  IMPORTGENCONID,
  CAST(FCASEXPORTLIMIT AS DOUBLE) AS FCASEXPORTLIMIT,
  CAST(FCASIMPORTLIMIT AS DOUBLE) AS FCASIMPORTLIMIT,
  CAST(LOCAL_PRICE_ADJUSTMENT_EXPORT AS DOUBLE) AS LOCAL_PRICE_ADJUSTMENT_EXPORT,
  CAST(LOCALLY_CONSTRAINED_EXPORT AS DOUBLE) AS LOCALLY_CONSTRAINED_EXPORT,
  CAST(LOCAL_PRICE_ADJUSTMENT_IMPORT AS DOUBLE) AS LOCAL_PRICE_ADJUSTMENT_IMPORT,
  CAST(LOCALLY_CONSTRAINED_IMPORT AS DOUBLE) AS LOCALLY_CONSTRAINED_IMPORT,
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR
FROM interconnector_staging
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
