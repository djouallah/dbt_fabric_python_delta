{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by=['DATE'],
    pre_hook="SET VARIABLE scada_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'scada_today'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} LIMIT {{ env_var('process_limit', '1000') }}))"
) }}

{#-- append, not merge/insert: the pre_hook only loads files NOT already in {{ this }}, so
     dedup is done in SQL and a key-join merge is redundant. append is a plain streaming
     append; dedup relies on that NOT IN {{ this }} filter under a single writer (no concurrent
     write), so no duplicate files slip in. ~288 intraday files/day land cheaply. --#}

{% set csv_archive_path = get_csv_archive_path() %}

{#-- Skip the file read entirely when no new scada_today files have arrived: otherwise the
     pre_hook's COALESCE(..., ['']) sentinel makes read_csv('') run against an empty path. --#}
{%- set check_files_query -%}
SELECT COUNT(*) as cnt FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'scada_today'
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
WITH scada_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('scada_today_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I': 'VARCHAR',
      'DISPATCH': 'VARCHAR',
      'UNIT_SCADA': 'VARCHAR',
      'xx': 'VARCHAR',
      'SETTLEMENTDATE': 'timestamp',
      'DUID': 'VARCHAR',
      'SCADAVALUE': 'double',
      'LASTCHANGED': 'timestamp'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND SCADAVALUE != 0
)

SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(LASTCHANGED AS TIMESTAMPTZ) AS LASTCHANGED,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM scada_staging
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
