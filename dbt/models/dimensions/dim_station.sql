-- Station entity: the physical site a DUID sits behind, rolled up from dim_duid.
--
-- Station name is the grain, so the rare case of two owners using the same station
-- name collapses into one row; ParticipantCount makes that visible rather than
-- silent. WA facilities carry no station name in the source and are excluded.
--
-- The DispatchType counts are what answers "which stations have both a generating
-- and a load unit". Note the modern battery fleet does NOT look like that: batteries
-- now register a single 'Bidirectional Unit' rather than a gen DUID plus a load DUID,
-- so a gen+load pair today means pumped hydro and a handful of legacy sites.
SELECT
  StationName,
  first(Participant)   AS Participant,
  first(Region)        AS Region,
  count(DISTINCT Participant) AS ParticipantCount,
  count(*)             AS UnitCount,
  count(*) FILTER (WHERE DispatchType = 'Generating Unit')   AS GeneratingUnitCount,
  count(*) FILTER (WHERE DispatchType = 'Load')              AS LoadUnitCount,
  count(*) FILTER (WHERE DispatchType = 'Bidirectional Unit') AS BidirectionalUnitCount,
  sum(RegCapMW)        AS RegCapMW
FROM {{ ref('dim_duid') }}
WHERE StationName IS NOT NULL AND StationName <> ''
GROUP BY StationName
