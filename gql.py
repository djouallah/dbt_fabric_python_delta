# GQL verification harness against the aemo_nem_v5 graph.
#
# Expected results in the labels are hand-verified ground truth -- the same numbers come out
# of SQL over the same Delta tables (mart.dim_*, mart.fct_region, mart.fct_summary,
# mart.fct_interconnector_derived). If a query here drifts, either the graph load broke or
# the marts changed shape.
#
# v5 renamed the two entities whose names were GQL reserved words: `Unit` -> GeneratingUnit
# and `Interval` -> TimeHHMM. Nothing below needs a backtick, which is the whole point of
# the rename -- a bare (u:Unit) used to fail as a SYNTAX error and read like a broken
# binding.

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
ONTOLOGY  = "aemo_nem_v5"
API       = "https://api.fabric.microsoft.com/v1"

QUERIES = [
    # --- structure: ownership, topology, co-location -------------------------------------
    ("AGL direct children (8)",
     "MATCH (child:Participant)-[:SUBSIDIARY_OF]->(p:Participant) "
     "WHERE p.Participant = 'AGL Energy Limited' RETURN child.Participant"),

    ("AGL incl. subsidiaries (41 units / 8850.58 MW)",
     "MATCH (u:GeneratingUnit)-[:OPERATED_BY]->(sub:Participant)"
     "-[:SUBSIDIARY_OF]->{1,5}(p:Participant) "
     "WHERE p.Participant = 'AGL Energy Limited' "
     "RETURN count(DISTINCT u.DUID) AS units, sum(u.RegCapMW) AS mw"),

    ("EnergyAustralia ownership chain (Holdings -> CLP)",
     "MATCH (p:Participant)-[:SUBSIDIARY_OF]->{1,5}(anc:Participant) "
     "WHERE p.Participant = 'EnergyAustralia Pty Ltd' RETURN anc.Participant"),

    ("TAS1 participants -- who a Basslink trip isolates (5)",
     "MATCH (u:GeneratingUnit)-[:OPERATED_BY]->(p:Participant) "
     "WHERE u.Region = 'TAS1' RETURN DISTINCT p.Participant"),

    ("Regions one in-service hop from TAS1 (VIC1 via Basslink)",
     "MATCH (r1:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)"
     "-[:CONNECTS_FROM|CONNECTS_TO]->(r2:Region) "
     "WHERE r1.RegionID = 'TAS1' AND i.InService "
     "AND r2.RegionID <> 'TAS1' RETURN DISTINCT r2.RegionID, i.Name"),

    ("Stations with both a generating and a load unit (3)",
     "MATCH (s:Station) WHERE s.GeneratingUnitCount > 0 AND s.LoadUnitCount > 0 "
     "RETURN s.StationName, s.GeneratingUnitCount, s.LoadUnitCount"),

    # --- v5 measures: the regional truth v4 could not express -----------------------------
    ("SA1 avg price 2026-08-09 from RegionInterval ($35.665 over 288 intervals). "
     "Averaging Observation.Price instead gives 35.145 -- a UNIT-WEIGHTED mean over "
     "15,032 rows, which is the wrong question",
     "MATCH (ri:RegionInterval) WHERE ri.RegionID = 'SA1' AND ri.DateKey = '2026-08-09' "
     "RETURN avg(ri.Price) AS price, count(ri.Price) AS intervals"),

    ("QLD1 demand, week to 2026-08-11 (avg 5915.88, peak 8071.7, NI +797.15, "
     "1777 intervals)",
     "MATCH (ri:RegionInterval) WHERE ri.RegionID = 'QLD1' "
     "AND ri.DateKey >= '2026-08-05' AND ri.DateKey <= '2026-08-11' "
     "RETURN avg(ri.TotalDemand) AS avg_demand, max(ri.TotalDemand) AS peak_demand, "
     "avg(ri.NetInterchange) AS avg_net_export, count(ri.TotalDemand) AS intervals"),

    ("Corridor flows, week to 2026-08-11 (QLD1-NSW1 -797.15, VIC1-NSW1 +444.37, "
     "SA1-VIC1 -32.60, TAS1-VIC1 -125.15)",
     "MATCH (f:Flow) WHERE f.DateKey >= '2026-08-05' AND f.DateKey <= '2026-08-11' "
     "RETURN f.LinkID AS link, avg(f.FlowMW) AS avg_mw GROUP BY link ORDER BY link"),

    ("SA1 generation 2026-08-09 (46,351.5 MWh, 15,032 readings, 66 units). "
     "Observation carries RegionID, so no join is needed",
     "MATCH (o:Observation) WHERE o.RegionID = 'SA1' AND o.DateKey = '2026-08-09' "
     "RETURN sum(o.MW) * 5 / 60 AS mwh, count(o.MW) AS readings, "
     "count(DISTINCT o.DUID) AS units"),

    ("Region -[:OBSERVED]-> RegionInterval traverses (TAS1, 288 intervals)",
     "MATCH (r:Region)-[:OBSERVED]->(ri:RegionInterval) "
     "WHERE r.RegionID = 'TAS1' AND ri.DateKey = '2026-08-09' "
     "RETURN avg(ri.TotalDemand) AS demand, count(ri.TotalDemand) AS intervals"),
]

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {get_fabric_token()}",
                        "Content-Type": "application/json", "Accept": "application/json"})

models = session.get(f"{API}/workspaces/{WORKSPACE}/GraphModels").json()["value"]
graph = next(m for m in models if ONTOLOGY in m["displayName"])
url = f"{API}/workspaces/{WORKSPACE}/GraphModels/{graph['id']}/executeQuery?preview=true"
print(f"graph: {graph['displayName']}\n")

for label, query in QUERIES:
    print("=" * 74)
    print(label)
    body = session.post(url, json={"query": query}, timeout=600).json()
    status = body.get("status", {})
    # Application errors come back as HTTP 200 -- the status code is what matters, so
    # raise_for_status() would report success on a failed query.
    if not str(status.get("code", "")).startswith(("00", "01", "02", "03")):
        print(f"  FAILED {status.get('code')}: "
              f"{(status.get('cause') or {}).get('description', '')[:200]}")
        continue
    rows = body["result"]["data"]
    # The FIRST heavy query after a graph load can return status 00000 with EMPTY data --
    # treat an empty aggregate as suspect rather than as a real zero.
    if not rows:
        print("    (empty -- suspect right after a load; re-run before believing it)")
    for row in rows[:10]:
        print("   ", row)