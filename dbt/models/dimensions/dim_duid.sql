{% set csv_archive_path = get_csv_archive_path() %}

{{ config(
    materialized='table'
) }}

-- Ensure download runs first by depending on stg_csv_archive_log
-- {{ ref('stg_csv_archive_log') }}

WITH
  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS RegionID,
      first("Fuel Source - Descriptor") AS FuelSource,
      trim(first(Participant)) AS Participant,
      trim(first("Station Name")) AS StationName,
      first("Dispatch Type") AS DispatchType,
      first("Technology Type - Descriptor") AS Technology,
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
      'WA1' AS RegionID,
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
      wa_energy.Technology AS FuelSource,
      trim(wa_facilities.Participant) AS Participant,
      -- The WEM facilities feed carries no station, dispatch type, technology or
      -- registered capacity; NULL rather than a guess.
      NULL::VARCHAR AS StationName,
      NULL::VARCHAR AS DispatchType,
      NULL::VARCHAR AS Technology,
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
  first(a.RegionID) AS RegionID,
  first(UPPER(LEFT(TRIM(FuelSource), 1)) || LOWER(SUBSTR(TRIM(FuelSource), 2))) AS FuelSource,
  first(a.Participant) AS Participant,
  first(regions.State) AS State,
  first(a.StationName) AS StationName,
  first(a.DispatchType) AS DispatchType,
  first(a.Technology) AS Technology,
  max(a.RegCapMW) AS RegCapMW,
  first(geo.latitude) AS latitude,
  first(geo.longitude) AS longitude
FROM duid_all a
JOIN {{ ref('dim_region') }} regions ON a.RegionID = regions.RegionID
LEFT JOIN geo ON a.duid = geo.duid
GROUP BY a.DUID
