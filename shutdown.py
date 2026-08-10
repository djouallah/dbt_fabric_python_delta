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
    body = session.post(url, json={"query": query}).json()
    status = body.get("status", {})
    if not str(status.get("code", "")).startswith(("00", "01", "02", "03")):
        raise RuntimeError(f"{status.get('code')} "
                           f"{(status.get('cause') or {}).get('description', '')[:200]}")
    return body["result"]["data"]


def neighbours(region, broken):
    """Regions one hop from `region`, pretending `broken` is out of service.

    Interconnector is a NODE (so it can carry InService/AcDc), which makes a
    region-to-region hop TWO edges. GQL rejects a quantified pattern spanning that
    -- "Parenthesized path pattern expressions must be formed of exactly one edge
    pattern in between two node patterns" -- so reachability is walked one explicit
    hop at a time and closed below. Modelling the link as an edge instead would allow
    {1,n} quantification but would lose the ability to filter an individual link,
    since relationship instances carry no properties.
    """
    rows = gql(
        "MATCH (a:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)"
        "-[:CONNECTS_FROM|CONNECTS_TO]->(b:Region) "
        f"WHERE a.RegionID = '{region}' AND i.InterconnectorInService "
        f"AND i.InterconnectorID <> '{broken}' AND b.RegionID <> '{region}' "
        "RETURN DISTINCT b.RegionID AS r")
    return {row["r"] for row in rows}


links = gql("MATCH (i:Interconnector) WHERE i.InterconnectorInService "
            "RETURN i.InterconnectorID AS id, i.InterconnectorName AS name")
regions = {r["r"] for r in
           gql("MATCH (r:Region) WHERE r.RegionMarket = 'NEM' RETURN r.RegionID AS r")}
# DateKey is already an ISO string, so no [:10] truncation dance.
day = gql("MATCH (ud:UnitDay) WHERE ud.UnitDayIntervalCount = 288 "
          "RETURN max(ud.UnitDayDateKey) AS d")[0]["d"]

print(f"{len(links)} in-service links, {len(regions)} NEM regions")
print(f"generation priced on {day} (latest complete day)\n")
print(f"{'link cut':<26} {'stranded regions':<24} {'MW':>8} {'MWh/day':>10}")
print("-" * 72)

for link in sorted(links, key=lambda l: l["name"]):
    reached, frontier = {ROOT}, {ROOT}
    while frontier:                       # transitive closure; <= 4 rounds for 5 regions
        frontier = set().union(*(neighbours(r, link["id"]) for r in frontier)) - reached
        reached |= frontier
    stranded = regions - reached
    if not stranded:
        print(f"{link['name']:<26} {'-- nothing --':<24}")
        continue
    mw = gql("MATCH (u:`Unit`) WHERE "                     # `Unit` is a GQL reserved word
             + " OR ".join(f"u.UnitRegion = '{r}'" for r in stranded)
             + " RETURN sum(u.UnitRegCapMW) AS mw")[0]["mw"]
    # Region x day is DERIVED: aggregate UnitDay through Unit.UnitRegion. There is no
    # RegionDay entity -- it would only duplicate what this traversal computes.
    mwh = gql(f"MATCH (ud:UnitDay)<-[:PRODUCED]-(u:`Unit`) "
              f"WHERE ud.UnitDayDateKey = '{day}' AND ("
              + " OR ".join(f"u.UnitRegion = '{r}'" for r in stranded)
              + ") RETURN sum(ud.UnitDayGenerationMWh) AS mwh")[0]["mwh"]
    print(f"{link['name']:<26} {', '.join(sorted(stranded)):<24} "
          f"{float(mw):>8,.0f} {float(mwh):>10,.0f}")
