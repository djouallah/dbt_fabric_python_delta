# N-1 contingency on the NEM interconnector network, from the graph.
#
# For each in-service interconnector: remove it, compute what can still reach NSW1, and
# report the regions left stranded -- plus what is actually at stake there.
#
# v5 gives this a number it could not have before. The old version could only report
# generation CAPACITY at risk (a static dim_duid property). RegionInterval carries
# TotalDemand, so it can now also report the DEMAND that would be islanded -- which is the
# number that matters to consumers, and it is measured rather than inferred.
#
# Why the reachability walk is one explicit hop at a time: Interconnector is a NODE (so it
# can carry InService/AcDc), which makes a region-to-region hop TWO edges. GQL rejects a
# quantified pattern spanning that -- "Parenthesized path pattern expressions must be formed
# of exactly one edge pattern in between two node patterns" -- so the closure is walked here
# in Python. Modelling the link as an edge instead would allow {1,n} quantification but lose
# per-link filtering, since relationship instances carry no properties.
#
# Note QNI and Terranora both join NSW1<->QLD1, so cutting either one alone strands nothing;
# the walk handles that for free.

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
ONTOLOGY  = "aemo_nem"
API       = "https://api.fabric.microsoft.com/v1"
ROOT      = "NSW1"   # reference region: whatever cannot reach it is stranded

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {get_fabric_token()}",
                        "Content-Type": "application/json", "Accept": "application/json"})

models = session.get(f"{API}/workspaces/{WORKSPACE}/GraphModels").json()["value"]
graph = next(m for m in models if ONTOLOGY in m["displayName"])
url = f"{API}/workspaces/{WORKSPACE}/GraphModels/{graph['id']}/executeQuery?preview=true"


def gql(query):
    """Run a GQL query. Application errors arrive as HTTP 200 with the failure in
    status.code, so raise_for_status() would report success on a failed query."""
    body = session.post(url, json={"query": query}, timeout=600).json()
    status = body.get("status", {})
    if not str(status.get("code", "")).startswith(("00", "01", "02", "03")):
        raise RuntimeError(f"{status.get('code')} "
                           f"{(status.get('cause') or {}).get('description', '')[:200]}")
    return body["result"]["data"]


def neighbours(region, broken):
    """Regions one hop from `region`, pretending `broken` is out of service."""
    rows = gql(
        "MATCH (a:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)"
        "-[:CONNECTS_FROM|CONNECTS_TO]->(b:Region) "
        f"WHERE a.RegionID = '{region}' AND i.InService "
        f"AND i.InterconnectorID <> '{broken}' AND b.RegionID <> '{region}' "
        "RETURN DISTINCT b.RegionID AS r")
    return {row["r"] for row in rows}


links = gql("MATCH (i:Interconnector) WHERE i.InService "
            "RETURN i.InterconnectorID AS id, i.Name AS name")
regions = {r["r"] for r in
           gql("MATCH (r:Region) WHERE r.Market = 'NEM' RETURN r.RegionID AS r")}
# DateKey is already a YYYYMMDD integer, so no truncation dance. RegionInterval is ~30x smaller
# than Observation, so it is the cheap place to ask what the latest day is.
day = gql("MATCH (ri:RegionInterval) RETURN max(ri.DateKey) AS d")[0]["d"]

print(f"{len(links)} in-service links, {len(regions)} NEM regions")
print(f"demand measured on {day} (latest day in the data)\n")
print(f"{'link cut':<26} {'stranded regions':<24} {'cap MW':>9} {'demand MW':>11} "
      f"{'intervals':>10}")
print("-" * 85)

for link in sorted(links, key=lambda l: l["name"]):
    reached, frontier = {ROOT}, {ROOT}
    while frontier:                       # transitive closure; <= 4 rounds for 5 regions
        frontier = set().union(*(neighbours(r, link["id"]) for r in frontier)) - reached
        reached |= frontier
    stranded = regions - reached
    if not stranded:
        print(f"{link['name']:<26} {'-- nothing --':<24}")
        continue

    where_unit = " OR ".join(f"u.RegionID = '{r}'" for r in stranded)
    cap = gql(f"MATCH (u:GeneratingUnit) WHERE {where_unit} "
              "RETURN sum(u.RegCapMW) AS mw")[0]["mw"]
    # The demand that gets islanded. Demand is instantaneous MW, so it is AVERAGED over the
    # day's intervals per region and the per-region averages are then added. Do NOT divide a
    # summed total by 288: the latest day is usually PARTIAL (today's intraday tail), so the
    # true interval count is whatever avg() sees, not a full day's worth.
    where_region = " OR ".join(f"ri.RegionID = '{r}'" for r in stranded)
    rows = gql(f"MATCH (ri:RegionInterval) WHERE ri.DateKey = {day} "
               f"AND ({where_region}) "
               "RETURN ri.RegionID AS r, avg(ri.TotalDemand) AS mw, "
               "count(ri.TotalDemand) AS n GROUP BY r")
    demand = sum(float(row["mw"]) for row in rows)
    intervals = min(int(row["n"]) for row in rows)

    print(f"{link['name']:<26} {', '.join(sorted(stranded)):<24} "
          f"{float(cap):>9,.0f} {demand:>11,.0f} {intervals:>10,}")
