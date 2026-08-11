-- The sum of every region's NetInterchange must be POSITIVE: it is NEM-wide transmission
-- loss, and losses cannot be negative. Observed range on real data is +16 to +313 MW
-- (average +123).
--
-- This is the tripwire for fct_interconnector_derived, which solves every region-pair flow
-- from NetInterchange alone. That solve is valid only while the network is a TREE
-- (QLD1-NSW1-VIC1-SA1 with TAS1 off VIC1). Two distinct failures make it fire:
--
--   1. A sign-convention regression in fct_region (NetInterchange flipping to
--      positive-means-import) sends the residual negative immediately.
--   2. EnergyConnect (PEC, SA1<->NSW1) commissioning closes a cycle. Once power flows on it,
--      net injections no longer determine edge flows and the residual stops behaving like a
--      pure loss term.
--
-- If this fails for reason 2, fct_interconnector_derived must be RETIRED in favour of
-- fct_interconnector, not patched.
--
-- A tolerance of -1 MW rather than 0 absorbs rounding in AEMO's own published figures.
SELECT
  date,
  time,
  residual_mw
FROM (
  SELECT
    date,
    time,
    SUM(NetInterchange) AS residual_mw
  FROM {{ ref('fct_region') }}
  WHERE RegionID IN ('NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1')
  GROUP BY date, time
)
WHERE residual_mw < -1
