{#-- Region-pair power flows for the FULL history, reconstructed from NetInterchange alone.

     WHY THIS EXISTS: per-link flow (INTERCONNECTORRES) is only published in the DISPATCHIS
     Current report, a ~2-day rolling window. The PUBLIC_DAILY archive that gives us years of
     history has no interconnector record at all. But it does not need one.

     THE TRICK: the NEM is a TREE.
         QLD1 --- NSW1 --- VIC1 --- SA1
                            |
                           TAS1
     A tree with n nodes has exactly n-1 edges, so n-1 independent net injections determine
     every edge flow uniquely. NetInterchange (net export per region) is therefore sufficient:

         QLD1->NSW1 = -NI(QLD1)      (QLD's only neighbour is NSW)
         SA1 ->VIC1 = -NI(SA1)       (SA's only neighbour is VIC while PEC is out of service)
         TAS1->VIC1 = -NI(TAS1)      (TAS's only neighbour is VIC)
         VIC1->NSW1 =  NI(VIC1) + NI(SA1) + NI(TAS1)
                                     (VIC's net export, less what it takes in from SA and TAS)

     VERIFIED, not assumed. sum(NetInterchange) over all five regions averages +122.9 MW and
     is ALWAYS positive (observed 16.1 to 312.8) -- that residual is transmission loss, which
     physics requires to be positive. A sign error would swing it negative. Second check:
     TAS1's extreme import over the whole history is -478.0 MW, which is Basslink's nameplate
     rating to the megawatt.

     THE CAVEAT THAT WILL EVENTUALLY BITE: this is exact only while the network is a TREE.
     EnergyConnect (PEC, SA1<->NSW1) is currently dim_interconnector.InService = false. The
     day it commissions, the graph gains a cycle, flows stop being recoverable from net
     injections, and this model must be RETIRED in favour of fct_interconnector. The singular
     test assert_network_residual_is_loss.sql is the tripwire: once PEC carries power the
     residual stops behaving like a loss term.

     Grain is region-PAIR, not physical link: QNI and Terranora share the NSW1-QLD1 corridor
     and cannot be separated from net injections, nor can Heywood from Murraylink. --#}
{{ config(
    materialized='table',
    schema='mart'
) }}

WITH ni AS (
  SELECT
    date,
    TimeHHMM,
    month_key,
    MAX(CASE WHEN RegionID = 'NSW1' THEN NetInterchange END) AS nsw,
    MAX(CASE WHEN RegionID = 'QLD1' THEN NetInterchange END) AS qld,
    MAX(CASE WHEN RegionID = 'SA1'  THEN NetInterchange END) AS sa,
    MAX(CASE WHEN RegionID = 'TAS1' THEN NetInterchange END) AS tas,
    MAX(CASE WHEN RegionID = 'VIC1' THEN NetInterchange END) AS vic
  FROM {{ ref('fct_region') }}
  WHERE RegionID IN ('NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1')
  GROUP BY ALL
),

solved AS (
  SELECT date, TimeHHMM, month_key,
         nsw + qld + sa + tas + vic AS residual,
         'QLD1-NSW1' AS LinkID, 'QLD1' AS FromRegionID, 'NSW1' AS ToRegionID,
         'QNI + Terranora' AS LinkName, -qld AS FlowMW
  FROM ni
  UNION ALL
  SELECT date, TimeHHMM, month_key, nsw + qld + sa + tas + vic,
         'VIC1-NSW1', 'VIC1', 'NSW1', 'VNI', vic + sa + tas
  FROM ni
  UNION ALL
  SELECT date, TimeHHMM, month_key, nsw + qld + sa + tas + vic,
         'SA1-VIC1', 'SA1', 'VIC1', 'Heywood + Murraylink', -sa
  FROM ni
  UNION ALL
  SELECT date, TimeHHMM, month_key, nsw + qld + sa + tas + vic,
         'TAS1-VIC1', 'TAS1', 'VIC1', 'Basslink', -tas
  FROM ni
)

SELECT
  date,
  TimeHHMM,
  LinkID,
  CAST(date AS VARCHAR) AS DateKey,
  FromRegionID,
  ToRegionID,
  LinkName,
  -- Positive = flow in the FromRegionID -> ToRegionID direction.
  -- DOUBLE, never DECIMAL(p,s): see fct_region.
  CAST(FlowMW AS DOUBLE) AS FlowMW,
  -- Same value on all four rows of an interval: sum of every region's net injection, which
  -- is the NEM-wide transmission loss. Carried per row so the invariant stays inspectable
  -- without re-deriving it.
  CAST(residual AS DOUBLE) AS NetworkLossMW,
  month_key
FROM solved
WHERE FlowMW IS NOT NULL
