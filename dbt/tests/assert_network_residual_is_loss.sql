-- The sum of every region's NetInterchange is NEM-wide transmission loss, so it must behave
-- like a loss: positive on the whole, and never far negative on any interval.
--
-- This is the tripwire for fct_interconnector_derived, which solves every region-pair flow
-- from NetInterchange alone. That solve is valid only while the network is a TREE
-- (QLD1-NSW1-VIC1-SA1 with TAS1 off VIC1). Two distinct failures must fire it:
--
--   1. A sign-convention regression in fct_region (NetInterchange flipping to
--      positive-means-import) inverts every flow answer in the warehouse. It would drag the
--      mean from +86 to -86, so `mean_residual_not_positive` catches it.
--   2. EnergyConnect (PEC, SA1<->NSW1) commissioning closes a cycle. Once real power flows
--      on it, net injections no longer determine edge flows and the balance breaks by
--      hundreds of MW, which `interval_residual_far_negative` catches.
--
-- If this fails for reason 2, fct_interconnector_derived must be RETIRED in favour of
-- fct_interconnector, not patched.
--
-- WHY -50 AND NOT 0: measured over 74,592 intervals of real data, the residual has
-- mean +85.8, median +80.4, max +369.7 and min -19.4. It dips slightly below zero on 531
-- intervals (0.71%) and NEVER below -25. Those dips cluster in the 500-1500 MW interchange
-- band -- when little power is moving, AEMO's per-region loss allocation rounds to a small
-- negative. That is a measurement noise floor, not a broken invariant, so the per-interval
-- bound sits at -50: about 2.5x the worst noise ever observed, and orders of magnitude
-- tighter than either real failure above. An earlier version of this test asserted
-- residual >= -1 per interval and failed on 421 rows of ordinary noise.

WITH interval_residual AS (
  SELECT
    date,
    time,
    SUM(NetInterchange) AS residual
  FROM {{ ref('fct_region') }}
  WHERE RegionID IN ('NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1')
  GROUP BY date, time
)

-- Structural break: an interval far outside the measured noise floor.
SELECT
  'interval_residual_far_negative' AS check_name,
  CAST(date AS VARCHAR) || ' ' || CAST(time AS VARCHAR) AS detail,
  ROUND(residual, 1) AS value_mw
FROM interval_residual
WHERE residual < -50

UNION ALL

-- Sign convention: losses cannot be negative on average.
SELECT
  'mean_residual_not_positive',
  'whole history',
  ROUND(AVG(residual), 1)
FROM interval_residual
HAVING AVG(residual) <= 0
