{% set csv_archive_path = get_csv_archive_path() %}

{# Check if there are new DUIDs not in the existing table #}
{%- set check_new_duids_query -%}
  SELECT count(*) as cnt FROM (
    SELECT DUID FROM read_csv('{{ csv_archive_path }}/duid/duid_data.csv') WHERE length(DUID) > 2
    UNION
    SELECT "Facility Code" AS DUID FROM read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ) source_duids
  WHERE DUID NOT IN (SELECT DUID FROM {{ this }})
{%- endset -%}

{# The new-DUID check alone cannot see a column that was added to this model after the
   table was first built: no new DUID means no rows written, so the added column would
   stay NULL forever. Probe the target's schema first and rebuild every row once when a
   column is missing. #}
{%- set check_columns_query -%}
  SELECT count(*) as cnt FROM (DESCRIBE SELECT * FROM {{ this }})
  WHERE column_name = 'StationName'
{%- endset -%}

{%- if execute and is_incremental() and flags.WHICH == 'run' -%}
  {%- set columns_result = run_query(check_columns_query) -%}
  {%- if not (columns_result and columns_result.rows[0][0] > 0) -%}
    {%- set has_new_duids = true -%}
  {%- else -%}
    {%- set result = run_query(check_new_duids_query) -%}
    {%- set has_new_duids = result and result.rows[0][0] > 0 -%}
  {%- endif -%}
{%- else -%}
  {%- set has_new_duids = true -%}
{%- endif -%}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='DUID',
    on_schema_change='sync_all_columns'
) }}

-- Ensure download runs first by depending on stg_csv_archive_log
-- {{ ref('stg_csv_archive_log') }}

{% if has_new_duids %}
WITH
  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      trim(first(Participant)) AS Participant,
      trim(first("Station Name")) AS StationName,
      first("Dispatch Type") AS DispatchType,
      first("Technology Type - Descriptor") AS TechnologyType,
      max(try_cast("Reg Cap generation (MW)" AS DOUBLE)) AS RegCapMW
    FROM
      read_csv('{{ csv_archive_path }}/duid/duid_data.csv')
    WHERE
      length(DUID) > 2
    GROUP BY
      DUID
  ),

  wa_facilities AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM
      read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ),

  wa_energy AS (
    SELECT *
    FROM read_csv_auto('{{ csv_archive_path }}/duid/WA_ENERGY.csv', header = 1)
  ),

  duid_wa AS (
    SELECT
      wa_facilities.DUID,
      wa_facilities.Region,
      wa_energy.Technology AS FuelSourceDescriptor,
      trim(wa_facilities.Participant) AS Participant,
      -- The WEM facilities feed carries no station, dispatch type, technology or
      -- registered capacity; NULL rather than a guess.
      NULL::VARCHAR AS StationName,
      NULL::VARCHAR AS DispatchType,
      NULL::VARCHAR AS TechnologyType,
      NULL::DOUBLE  AS RegCapMW
    FROM wa_facilities
    LEFT JOIN wa_energy ON wa_facilities.DUID = wa_energy.DUID
  ),

  duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
  ),

  geo AS (
    SELECT
      duid,
      max(latitude) as latitude,
      max(longitude) as longitude
    FROM read_csv('{{ csv_archive_path }}/duid/geo_data.csv')
    WHERE latitude IS NOT NULL
    GROUP BY duid
  )

SELECT
  a.DUID,
  first(a.Region) AS Region,
  first(UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2))) AS FuelSourceDescriptor,
  first(a.Participant) AS Participant,
  first(regions.State) AS State,
  first(a.StationName) AS StationName,
  first(a.DispatchType) AS DispatchType,
  first(a.TechnologyType) AS TechnologyType,
  max(a.RegCapMW) AS RegCapMW,
  first(geo.latitude) AS latitude,
  first(geo.longitude) AS longitude
FROM duid_all a
JOIN {{ ref('dim_region') }} regions ON a.Region = regions.RegionID
LEFT JOIN geo ON a.duid = geo.duid
GROUP BY a.DUID
{% else %}
-- No new DUIDs found, return empty result to keep existing data
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
