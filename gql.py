import requests
from duckrun.auth import get_fabric_token

WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
ONTOLOGY  = "aemo_nem"
API       = "https://api.fabric.microsoft.com/v1"

# `Unit` is a RESERVED WORD in Fabric GQL -- a bare (u:Unit) is a syntax error,
# not a "no such label" error, which makes it look like the binding failed.
# Backtick it. Expected results are the hand-verified ground truth; the same
# numbers come out of a recursive CTE over the same Delta tables.
QUERIES = [
    ("AGL direct children (8)",
     "MATCH (child:Participant)-[:SUBSIDIARY_OF]->(p:Participant) "
     "WHERE p.Participant = 'AGL Energy Limited' RETURN child.Participant"),

    ("AGL incl. subsidiaries (41 units / 8850.6 MW)",
     "MATCH (u:`Unit`)-[:OPERATED_BY]->(sub:Participant)"
     "-[:SUBSIDIARY_OF]->{1,5}(p:Participant) "
     "WHERE p.Participant = 'AGL Energy Limited' "
     "RETURN count(DISTINCT u.DUID) AS units, sum(u.UnitRegCapMW) AS mw"),

    ("EnergyAustralia ownership chain (Holdings -> CLP)",
     "MATCH (p:Participant)-[:SUBSIDIARY_OF]->{1,5}(anc:Participant) "
     "WHERE p.Participant = 'EnergyAustralia Pty Ltd' RETURN anc.Participant"),

    ("TAS1 participants -- who a Basslink trip isolates (5)",
     "MATCH (u:`Unit`)-[:OPERATED_BY]->(p:Participant) "
     "WHERE u.UnitRegion = 'TAS1' RETURN DISTINCT p.Participant"),

    ("Regions one in-service hop from TAS1 (VIC1 via Basslink)",
     "MATCH (r1:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)"
     "-[:CONNECTS_FROM|CONNECTS_TO]->(r2:Region) "
     "WHERE r1.RegionID = 'TAS1' AND i.InterconnectorInService "
     "AND r2.RegionID <> 'TAS1' RETURN DISTINCT r2.RegionID, i.InterconnectorName"),

    ("Stations with both a generating and a load unit (3)",
     "MATCH (s:Station) WHERE s.StationGeneratingUnits > 0 AND s.StationLoadUnits > 0 "
     "RETURN s.StationName, s.StationGeneratingUnits, s.StationLoadUnits"),
]

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {get_fabric_token()}",
                        "Content-Type": "application/json", "Accept": "application/json"})

models = session.get(f"{API}/workspaces/{WORKSPACE}/GraphModels").json()["value"]
graph = next(m for m in models if ONTOLOGY in m["displayName"])
url = f"{API}/workspaces/{WORKSPACE}/GraphModels/{graph['id']}/executeQuery?preview=true"
print(f"graph: {graph['displayName']}\n")

for label, query in QUERIES:
    print("=" * 70)
    print(label)
    body = session.post(url, json={"query": query}).json()
    status = body.get("status", {})
    # Application errors come back as HTTP 200 -- the status code is what matters.
    if not str(status.get("code", "")).startswith(("00", "01", "02", "03")):
        print(f"  FAILED {status.get('code')}: "
              f"{(status.get('cause') or {}).get('description', '')[:200]}")
        continue
    for row in body["result"]["data"]:
        print("   ", row)
