-- Test: fct_summary should not have NULL prices (indicates broken SCADA-to-price join via dim_duid region)
-- Returns rows where Price is NULL, which means the DUID's region didn't match any REGIONID in price data

SELECT
  DateKey,
  DUID,
  MW
FROM {{ ref('fct_summary') }}
WHERE Price IS NULL
LIMIT 10
