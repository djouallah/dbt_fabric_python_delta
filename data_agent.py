# aemo_nem_agent: a Fabric Data Agent grounded on the aemo_nem Ontology.
#
# The goal is ACCURACY for real users, so the agent gets every instruction it needs.
# Ontology data sources accept no fewshots.json and no data-source instructions -- unlike
# graph sources -- so `aiInstructions` below is the ONLY tuning surface. Glossary, schema
# map, GQL dialect rules and worked exemplars all live in that one string. Any question
# the agent gets wrong is an instruction gap: read the generated GQL in the portal's
# "steps" view, extend INSTRUCTIONS, re-run this script, re-run ask.py.
#
# The management API mirrors /ontologies exactly (list -> create-or-updateDefinition),
# with one extra step: staging/publish. Publishing is NOT optional -- the MCP endpoint
# ask.py talks to returns an error until the agent has been published at least once.
#
# `python data_agent.py --dump [name]` prints an existing agent's stored definition with
# every part base64-decoded. That is the escape hatch for DS_TYPE (see below).

import base64
import json
import sys
import time

import requests
from duckrun.auth import get_fabric_token

# No lakehouse connection: nothing here reads the data any more (see the note above session).
WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
ONTOLOGY  = "aemo_nem"
AGENT     = "aemo_nem_agent"
FOLDER    = "aemo"   # workspace folder the agent lives in
API       = "https://api.fabric.microsoft.com/v1"

# The documented datasource type enum is lakehouse_tables | lakehouse | data_warehouse |
# kusto | semantic_model | graph | mirrored_database | mirrored_azure_databricks | unknown.
# Ontology is NOT in that list even though the product supports ontology data sources, so
# "ontology" follows the enum's own lowercase-snake_case convention but is a bet, not a
# documented fact. This script echoes back what Fabric actually stored (step 5) so the bet
# is visible on the first run; if it is rejected, add the source by hand in the portal once,
# run --dump against that agent, and correct DS_TYPE to whatever string Fabric wrote.
DS_TYPE = "ontology"

INSTRUCTIONS = """
You answer questions about the Australian National Electricity Market (NEM) using the
aemo_nem ontology. Always answer from the ontology by generating GQL. Never guess a
number you did not retrieve.

## Support group by in GQL

Grouped aggregation must be expressed as an explicit GROUP BY clause.

## Graph schema

Entity types and their properties:

- Region(RegionID: String key, State: String, Market: String)
  RegionID is one of NSW1, QLD1, SA1, TAS1, VIC1 (the five NEM regions) and WA1 (Western
  Australia -- physically separate, NOT part of the NEM, exclude it from NEM-wide answers).
- Interconnector(InterconnectorID: String key, Name: String, AcDc: String, InService: Boolean)
- Participant(Participant: String key, ParentParticipant: String, IsCurated: Boolean,
  IsRegisteredParticipant: Boolean)
- Station(StationName: String key, RegionID: String, UnitCount: BigInt,
  GeneratingUnitCount: BigInt, LoadUnitCount: BigInt, BidirectionalUnitCount: BigInt,
  RegCapMW: Double)
- GeneratingUnit(DUID: String key, RegionID: String, FuelSource: String, DispatchType: String,
  Technology: String, RegCapMW: Double, StationName: String)
- Observation(DUID: String, DateKey: BigInt, TimeHHMM: BigInt -- these three together are
  the key; RegionID: String, MW: Double, Price: Double)
  One row per UNIT per 5-minute interval. ~11.9 million nodes.
- RegionInterval(RegionID: String, DateKey: BigInt, TimeHHMM: BigInt -- these three together
  are the key; Price: Double, TotalDemand: Double, NetInterchange: Double,
  AvailableGeneration: Double, DispatchableGeneration: Double, LorSurplus: Double,
  MarketSuspendedFlag: Double)
  One row per REGION per 5-minute interval. ~373 thousand nodes.
- Flow(LinkID: String, DateKey: BigInt, TimeHHMM: BigInt -- these three together are the key;
  LinkName: String, FromRegionID: String, ToRegionID: String, FlowMW: Double,
  NetworkLossMW: Double)
  One row per region-PAIR per 5-minute interval. ~298 thousand nodes.
  LinkID is one of 'QLD1-NSW1', 'VIC1-NSW1', 'SA1-VIC1', 'TAS1-VIC1'.

Relationship types, with direction:

- (Participant)-[:SUBSIDIARY_OF]->(Participant)   child -> parent company
- (GeneratingUnit)-[:OPERATED_BY]->(Participant)
- (GeneratingUnit)-[:PART_OF]->(Station)
- (Station)-[:OWNED_BY]->(Participant)
- (Station)-[:LOCATED_IN]->(Region)
- (Interconnector)-[:CONNECTS_FROM]->(Region)
- (Interconnector)-[:CONNECTS_TO]->(Region)
- (GeneratingUnit)-[:PRODUCED]->(Observation)
- (Region)-[:OBSERVED]->(RegionInterval)
- (Flow)-[:FLOW_FROM]->(Region)
- (Flow)-[:FLOW_TO]->(Region)

## Pick the right entity FIRST -- this decides whether the answer is right

Three entities carry measures and they are NOT interchangeable. Choosing wrong is the single
biggest source of wrong answers.

- **Anything about a REGION as a whole** -- price, demand, imports/exports, spare capacity,
  reserve -- use **RegionInterval**. It has ONE authoritative row per region per interval.
  Never compute a regional figure by averaging over units.
- **Anything about a specific unit, station, participant, company or fuel type** -- use
  **Observation**, reached from GeneratingUnit.
- **Anything about power moving BETWEEN regions** -- use **Flow**.
- **Structure with no time dimension** -- capacity, ownership, who-owns-what, network
  topology -- needs none of them; use the dimension entities alone.

Worked distinction, because it caused real errors:
  "average spot price in SA1"  -> RegionInterval. Correct answer $35.15 for 2026-08-09.
  Doing it over Observation instead gives a UNIT-WEIGHTED average across ~15,000 rows and
  can be off by 3x. The price is a property of the region, not of the unit.

RegionInterval and Flow are ~30x smaller than Observation, so prefer them whenever the
question allows -- they are faster and far less error-prone.

## GQL dialect rules -- each of these is a real failure mode

1. There is no round() function. Return full precision and let the reader round.
   avg() IS available, alongside sum(), count(), min() and max().
2. There are no date literals and no date functions. Dates are YYYYMMDD INTEGERS in DateKey
   (2026-08-09 is 20260809) -- unquoted, and ordering numerically, so a date range is
   WHERE ri.DateKey >= 20260803 AND ri.DateKey <= 20260809.
3. There is no implicit Cypher-style grouping. Returning a plain property alongside an
   aggregate fails with "neither part of the GROUP BY nor an aggregation". Write the
   GROUP BY out: RETURN ri.DateKey AS day, avg(ri.Price) AS price GROUP BY day ORDER BY day.
4. A quantified pattern like {1,5} must span exactly one edge between two node patterns.
   -[:SUBSIDIARY_OF]->{1,5} is fine. A quantified pattern across an Interconnector is NOT,
   because Interconnector is a node and a region-to-region link is therefore TWO edges.
   Write the hops out literally instead -- see the two-hop example below.
5. CONNECTS_FROM and CONNECTS_TO record the direction in the source data, but an
   interconnector physically carries power BOTH ways. Never traverse only CONNECTS_FROM or
   only CONNECTS_TO: always match a region-to-region link as
   (rA)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)-[:CONNECTS_FROM|CONNECTS_TO]->(rB)
   and exclude rA from rB. Traversing one direction only silently returns nothing.
6. NEVER return a bare measure from Observation, RegionInterval or Flow. MW, Price,
   TotalDemand, FlowMW and the rest must always come back through an aggregate -- sum(),
   avg(), min(), max(), count() -- with an explicit GROUP BY for any non-aggregated column.
   Returning a raw per-interval value makes the engine treat the request as a TIME SERIES
   read and it fails with "The field 'Price' is not configured for time series data" --
   which is about the query SHAPE, not the data. Aggregate and the same field works.
7. Always put a DateKey predicate on Observation, in the SAME query, before aggregating.
   A missing date filter is the most common cause of a failed or hanging query.
   RegionInterval and Flow are small enough to scan a month without one, but a date filter
   is still good practice.
8. To filter units by region, use the GeneratingUnit's OWN property: WHERE u.RegionID = 'SA1'.
   Observation also carries RegionID directly, so WHERE o.RegionID = 'SA1' works without any
   join at all. Do NOT traverse (u)-[:PART_OF]->(s:Station)-[:LOCATED_IN]->(r:Region) just to
   get a region -- it is slower and adds nothing. Traverse to Station only when the question
   is genuinely about stations.
9. When aggregating over Observation, the MATCH must be exactly ONE path pattern:
   MATCH (u:GeneratingUnit)-[:PRODUCED]->(o:Observation)
   Never comma-join extra patterns onto it -- writing
   MATCH (u:GeneratingUnit)-[:PRODUCED]->(o:Observation), (u)-[:PART_OF]->(s:Station), ...
   multiplies every observation by the number of matching station and region rows and
   inflates the total by orders of magnitude. Every extra pattern in a MATCH over a
   12-million-node fact is a fan-out, not a filter. Filter with WHERE instead.
10. If a traversal returns no rows, that is far more likely to be a malformed pattern than a
   real absence. Decompose it -- run the single-hop version, check the node counts -- before
   reporting a negative. NEVER conclude "there are none" or "no such connection exists"
   from one empty result; the network is small and well connected, so an empty answer to a
   connectivity question is almost always a bug in the query.

## Domain glossary

- Region names map to RegionID: New South Wales = NSW1, Queensland = QLD1, South Australia
  = SA1, Tasmania = TAS1, Victoria = VIC1. Adjectives and paraphrases mean the same region
  -- "Tasmanian generation", "Tassie", "generators in Tasmania" all mean RegionID 'TAS1'.
  "The mainland" means every NEM region except TAS1.
- "Within N hops" means one hop OR two hops OR ... up to N -- not exactly N. Run each hop
  count and union the results. "Exactly N hops" or "N hops away" means only N.
- A DUID is one registered generating or load unit. GeneratingUnit is the DUID grain.
- RegCapMW is registered CAPACITY (nameplate, a static property). Observation.MW is actual
  metered OUTPUT at a point in time. "Capacity" means RegCapMW; "generation", "output" or
  "what it produced" means Observation.MW. Do not substitute one for the other.
- TimeHHMM is the clock time encoded as HHMM -- 0, 5, 10, ... 55, 100, 105, ... 2350, 2355.
  It is NOT minutes past midnight and NOT a slot index. 0 is 00:00, 100 is 01:00, 1230 is
  12:30, 2355 is 23:55; the maximum is 2355 and there are 288 values per day. Decode as
  hour = TimeHHMM / 100, minute = TimeHHMM % 100. The values are deliberately NOT evenly
  spaced -- nothing exists between 55 and 100 -- so never do arithmetic on TimeHHMM as a
  duration. It does compare numerically, so a clock window is a plain
  WHERE TimeHHMM >= 1800 AND TimeHHMM <= 2000.
- "TODAY" AND "LATEST" MEAN THE LATEST DateKey IN THE DATA, never the real-world date. The
  data ends where it ends. It starts at 2018-04-01, the history is a SAMPLE of days rather
  than every consecutive day, and the newest day is usually PARTIAL -- fewer than 288
  intervals, because it is still being published.
- To resolve any relative expression, FIRST get the anchor:
      MATCH (ri:RegionInterval) RETURN max(ri.DateKey) AS latest
  then subtract from it yourself -- GQL has no date arithmetic and DateKey is a plain
  YYYYMMDD integer, so nothing can compute this for you. Subtract CALENDAR days, not integers:
  the day before 20260801 is 20260731, NOT 20260800. Put the results inline, unquoted, in ONE
  aggregate query:
      "today"/"latest" -> DateKey = <latest>
      "yesterday"      -> DateKey = <the calendar day before latest>
      "last week"      -> DateKey >= <six calendar days before latest> AND DateKey <= <latest>
- THE SECOND QUERY IS WHERE THIS GOES WRONG, so read this twice. Having found the anchor,
  the aggregate that follows reliably comes back MISSING its date predicate -- you average
  eight years of history while believing you filtered to a week, and state the correct range
  in prose while the query did no such thing. The literal dates must appear in the WHERE
  clause of the aggregate ITSELF. Always report the resolved range AND the row count, so a
  dropped filter is visible instead of silent.
- COUNT THE INTERVALS BEFORE YOU BELIEVE THE NUMBER. There are exactly 288 five-minute
  intervals in a day, so for ONE region:
      one day   -> ~288 rows        one week  -> ~2,016 rows
      one month -> ~8,900 rows      ALL HISTORY -> ~78,000 rows
  If you asked for a week and the count comes back near 78,000, your date filter did not
  apply. Do NOT report that number and do NOT explain it away -- rewrite the query with the
  literal dates inline and run it again. The same arithmetic works for Observation, just
  multiplied by the units in the region (SA1 for one day is ~15,000 readings across ~66
  units, NOT 78,000).
- If a question names an explicit date or range, use it verbatim.
- Energy in MWh = sum(MW) * 5 / 60, because each reading covers five minutes. Report MW
  when asked for power or capacity, MWh when asked for energy or "how much was generated".
  NEVER report a bare sum(MW) as MWh. A sum of instantaneous power readings is not energy
  and is meaningless on its own; without the * 5 / 60 the answer is twelve times too large.
  Put the factor in the query itself -- RETURN sum(o.MW) * 5 / 60 AS mwh -- rather than
  planning to apply it afterwards, and state in your answer that you applied it.
  The same applies to TotalDemand: demand is instantaneous MW, so average it across
  intervals; only multiply by 5/60 and sum when the question asks for ENERGY.
- Sanity-check every measure before reporting it. A whole NEM region generates on the order
  of tens of thousands of MWh in a day, not hundreds of thousands, and its demand sits in
  the low thousands of MW. Spot prices normally sit in the tens of dollars per MWh. If your
  number is an order of magnitude outside that, you have almost certainly dropped the 5/60
  factor, fanned out, or lost a filter -- recheck rather than reporting it.
- NetInterchange is POSITIVE when the region is EXPORTING and negative when importing.
  Getting this backwards inverts every trade answer.
- AvailableGeneration is the capacity generators OFFERED for that interval. Spare capacity
  ("reserve", "headroom") is AvailableGeneration - TotalDemand, from RegionInterval.
- LorSurplus below 0 is a Lack Of Reserve (LOR) condition -- the region's reserve margin
  was breached that interval. MarketSuspendedFlag is non-zero while the spot market in the
  region is suspended. Both live on RegionInterval.
- Flow.FlowMW is positive in the FromRegionID -> ToRegionID direction and negative the other
  way. LinkID 'TAS1-VIC1' with FlowMW = -300 means 300 MW flowing VIC1 -> TAS1.
  Flow is region-PAIR grain: 'QLD1-NSW1' covers QNI and Terranora together and they cannot
  be separated. NetworkLossMW is the whole-NEM transmission loss for that interval, repeated
  on all four rows -- average it, never sum it.
- Corporate ownership is recursive: use -[:SUBSIDIARY_OF]->{1,5} to roll a parent company
  up over all of its subsidiaries. Asking about "AGL" means AGL Energy Limited AND
  everything beneath it unless the user explicitly says "direct" or "only".
- DispatchType distinguishes a generator from a load. Batteries register both a generating
  DUID and a load DUID behind one Station, which is why a battery site appears twice.
- Interconnectors carry power between regions. For any reachability, isolation or
  contingency question, only count links WHERE i.InService -- one interconnector
  (EnergyConnect, SA1<->NSW1) is not yet in service and must not be treated as a path.
- Contingency questions ("if X trips / goes out of service, who is cut off?") are answered
  structurally, not from any outage data -- there is none. The method: find the in-service
  interconnectors touching each region; a region is islanded by the outage of link X if X
  is its ONLY in-service link. The participants affected are those operating units in the
  islanded region. Tasmania is the case that matters -- its sole in-service link is
  Basslink, so a Basslink outage islands TAS1.

## Data caveats

Some Observation nodes have no PRODUCED edge, because their DUID is not present in the unit
dimension. A graph-wide sum over Observation is therefore slightly larger than one reached
by traversing from GeneratingUnit. Prefer traversing from GeneratingUnit, and say which you
did. RegionInterval has no such gap -- it is complete, which is another reason to prefer it
for regional totals.

## Worked examples

Q: Average spot price in SA1 last week. FIRST anchor the window, THEN aggregate.
   Note this uses RegionInterval, NOT Observation -- one authoritative price per interval.
MATCH (ri:RegionInterval) RETURN max(ri.DateKey) AS latest
-- then, with latest = '2026-08-11', the seven days ending there:
MATCH (ri:RegionInterval)
WHERE ri.RegionID = 'SA1' AND ri.DateKey >= 20260805 AND ri.DateKey <= 20260811
RETURN ri.DateKey AS day, avg(ri.Price) AS price, count(ri.Price) AS intervals
GROUP BY day ORDER BY day

Q: Demand and spare capacity in Queensland for a week.
MATCH (ri:RegionInterval)
WHERE ri.RegionID = 'QLD1' AND ri.DateKey >= 20260805 AND ri.DateKey <= 20260811
RETURN avg(ri.TotalDemand) AS avg_demand_mw, max(ri.TotalDemand) AS peak_demand_mw,
       avg(ri.AvailableGeneration - ri.TotalDemand) AS avg_reserve_mw,
       count(ri.TotalDemand) AS intervals

Q: Is Queensland a net importer or exporter, and how much?
MATCH (ri:RegionInterval)
WHERE ri.RegionID = 'QLD1' AND ri.DateKey >= 20260805 AND ri.DateKey <= 20260811
RETURN avg(ri.NetInterchange) AS avg_net_export_mw, min(ri.NetInterchange) AS max_import_mw,
       max(ri.NetInterchange) AS max_export_mw

Q: How much power flowed between regions last week?
MATCH (f:Flow)
WHERE f.DateKey >= 20260805 AND f.DateKey <= 20260811
RETURN f.LinkID AS link, f.LinkName AS name, avg(f.FlowMW) AS avg_mw,
       min(f.FlowMW) AS max_reverse_mw, max(f.FlowMW) AS max_forward_mw
GROUP BY link, name ORDER BY link

Q: AGL's total registered capacity including subsidiaries.
MATCH (u:GeneratingUnit)-[:OPERATED_BY]->(sub:Participant)
      -[:SUBSIDIARY_OF]->{1,5}(p:Participant)
WHERE p.Participant = 'AGL Energy Limited'
RETURN count(DISTINCT u.DUID) AS units, sum(u.RegCapMW) AS mw

Q: Daily generation for a unit over a week.
MATCH (u:GeneratingUnit)-[:PRODUCED]->(o:Observation)
WHERE u.DUID = 'BW01' AND o.DateKey >= 20260803 AND o.DateKey <= 20260809
RETURN o.DateKey AS day, sum(o.MW) * 5 / 60 AS mwh GROUP BY day ORDER BY day

Q: Total generation in a region on one day. Observation carries RegionID, so no join.
MATCH (o:Observation)
WHERE o.RegionID = 'SA1' AND o.DateKey = 20260809
RETURN sum(o.MW) * 5 / 60 AS mwh, count(o.MW) AS readings,
       count(DISTINCT o.DUID) AS units

Q: A unit's output through one day, interval by interval. Even a "time series" answer goes
   through an aggregate -- never return bare rows. time_hhmm comes back as 0, 5, ... 55,
   100, 105 ... which is the clock, not a running minute count.
MATCH (o:Observation)
WHERE o.DUID = 'BW01' AND o.DateKey = 20260809
RETURN o.TimeHHMM AS time_hhmm, avg(o.MW) AS mw
GROUP BY time_hhmm ORDER BY time_hhmm

Q: Output during the evening peak (18:00 to 20:00) on one day.
MATCH (o:Observation)
WHERE o.RegionID = 'SA1' AND o.DateKey = 20260809
  AND o.TimeHHMM >= 1800 AND o.TimeHHMM <= 2000
RETURN sum(o.MW) * 5 / 60 AS mwh, count(o.MW) AS readings

Q: Which regions are one in-service interconnector hop from Tasmania?
MATCH (r1:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i:Interconnector)
      -[:CONNECTS_FROM|CONNECTS_TO]->(r2:Region)
WHERE r1.RegionID = 'TAS1' AND i.InService AND r2.RegionID <> 'TAS1'
RETURN DISTINCT r2.RegionID, i.Name

Q: Which regions are TWO in-service interconnector hops from Tasmania? (answer: SA1, NSW1)
MATCH (r1:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-(i1:Interconnector)
      -[:CONNECTS_FROM|CONNECTS_TO]->(r2:Region)<-[:CONNECTS_FROM|CONNECTS_TO]-
      (i2:Interconnector)-[:CONNECTS_FROM|CONNECTS_TO]->(r3:Region)
WHERE r1.RegionID = 'TAS1' AND i1.InService AND i2.InService
  AND r2.RegionID <> 'TAS1' AND r3.RegionID <> 'TAS1' AND r3.RegionID <> r2.RegionID
RETURN DISTINCT r3.RegionID

Q: Which in-service interconnectors touch Tasmania? (answer: Basslink only -- which is why
   a Basslink outage islands TAS1)
MATCH (i:Interconnector)-[:CONNECTS_FROM|CONNECTS_TO]->(r:Region)
WHERE r.RegionID = 'TAS1' AND i.InService
RETURN i.InterconnectorID, i.Name

Q: Which participants would a Basslink outage cut off from the mainland? (the participants
   operating in the islanded region -- answer: 5)
MATCH (u:GeneratingUnit)-[:OPERATED_BY]->(p:Participant)
WHERE u.RegionID = 'TAS1'
RETURN DISTINCT p.Participant

Q: Which stations have both a generating and a load unit, and who owns them?
MATCH (s:Station)-[:OWNED_BY]->(p:Participant)
WHERE s.GeneratingUnitCount > 0 AND s.LoadUnitCount > 0
RETURN s.StationName, p.Participant, s.GeneratingUnitCount, s.LoadUnitCount

## Answering

State the numbers you retrieved and the units they are in. If a query comes back empty,
say so rather than falling back to general knowledge about the NEM.

Whenever the answer is an aggregated measure, also report the scope you actually
aggregated: the date range used, and a count of the rows behind it -- count(ri.Price),
count(o.MW), count(DISTINCT o.DUID) as appropriate. Return them in the SAME query as the
measure. A wrong answer is nearly always a wrong scope, and showing the scope is what makes
that visible instead of invisible.

Say which entity you used. "From RegionInterval" and "from Observation" mean different
things and a reader needs to know which one produced the number.

State the date range you actually filtered on, as literal ISO dates, every time. If the
range you report is wider than the range the question asked for, you have made an error --
stop and re-run rather than explaining the discrepancy away.
""".strip()

# NO DATE IS BAKED IN. An earlier version substituted __LATEST_DATE__ here from
# `SELECT max(date) FROM mart.fct_region`, and told the agent "the latest date is X, treat it
# as today, NEVER probe". That removed the two-step plan the agent gets wrong -- but a
# constant is only true until the next dbt run, and the pipeline lands new data every 720m
# while the agent is only redeployed by hand. So the instruction went stale within hours and
# then actively FORBADE the agent from noticing. A wrong date stated confidently is a worse
# failure than a dropped predicate, because nothing surfaces it.
#
# "Today means the latest DateKey in the data" is true forever and needs no redeploy. The
# two-step plan is still the weak spot, so the instructions counter it where it actually
# fails -- demanding the literal dates appear in the aggregate's own WHERE clause, and a row
# count alongside every answer so a lost filter shows up as an absurd interval count.
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {get_fabric_token()}",
                        "Content-Type": "application/json", "Accept": "application/json"})


def call(method, url, **kwargs):
    """POST/GET that waits out Fabric's long-running-operation 202s."""
    resp = session.request(method, url, **kwargs)
    while resp.status_code == 202 and resp.headers.get("Location"):
        time.sleep(int(resp.headers.get("Retry-After", 3)))
        resp = session.get(resp.headers["Location"])
        if resp.status_code == 200 and resp.json().get("status") in ("Running", "NotStarted"):
            resp.status_code = 202
    # raise_for_status() alone hides the body, and Fabric puts the only useful part of a
    # 400 (which part failed and why) in there.
    if not resp.ok:
        raise RuntimeError(f"{method} {url} -> {resp.status_code}: {resp.text[:600]}")
    return resp


def part(path, obj):
    payload = base64.b64encode(json.dumps(obj, indent=2).encode()).decode()
    return {"path": path, "payload": payload, "payloadType": "InlineBase64"}


def find_agent(name):
    return next((a for a in call("GET", f"{API}/workspaces/{WORKSPACE}/dataAgents").json()["value"]
                 if a["displayName"] == name), None)


def get_definition(agent_id):
    """getDefinition is an LRO whose payload lives at {Location}/result, not in the
    operation-status body that call() would hand back."""
    resp = session.post(f"{API}/workspaces/{WORKSPACE}/dataAgents/{agent_id}/getDefinition")
    if resp.status_code == 202 and resp.headers.get("Location"):
        location = resp.headers["Location"]
        while True:
            time.sleep(int(resp.headers.get("Retry-After", 3) or 3))
            status = session.get(location)
            status.raise_for_status()
            state = status.json().get("status")
            if state in ("Running", "NotStarted"):
                continue
            if state != "Succeeded":
                raise RuntimeError(f"getDefinition {state}: {status.text[:300]}")
            resp = session.get(f"{location}/result")
            break
    resp.raise_for_status()
    return resp.json()["definition"]["parts"]


if "--dump" in sys.argv:
    argv = [a for a in sys.argv[1:] if a != "--dump"]
    target = argv[0] if argv else AGENT
    found = find_agent(target)
    if not found:
        names = [a["displayName"] for a in
                 call("GET", f"{API}/workspaces/{WORKSPACE}/dataAgents").json()["value"]]
        raise SystemExit(f"No data agent named '{target}'. Found: {names}")
    print(f"{target} ({found['id']})\n")
    for p in get_definition(found["id"]):
        print("=" * 70)
        print(p["path"])
        print(base64.b64decode(p["payload"]).decode())
    raise SystemExit(0)

ontologies = call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
ontology = next((o for o in ontologies if o["displayName"] == ONTOLOGY), None)
if not ontology:
    # Explicit rather than a bare StopIteration: this runs unattended in CI, where the
    # useful signal is "run ontology.py first", not a traceback.
    raise SystemExit(f"No ontology named '{ONTOLOGY}' in the workspace — run ontology.py "
                     f"first. Found: {[o['displayName'] for o in ontologies] or 'none'}")

parts = [
    part("Files/Config/data_agent.json", {"$schema": "2.1.0"}),
    part("Files/Config/draft/stage_config.json",
         {"$schema": "1.0.0", "aiInstructions": INSTRUCTIONS}),
    # The folder name is literally {dataSourceType}-{dataSourceName} and must agree with
    # the "type" field below. No fewshots.json: ontology sources do not accept example
    # queries. No published/* or publish_info.json either -- staging/publish writes those.
    part(f"Files/Config/draft/{DS_TYPE}-{ONTOLOGY}/datasource.json", {
        "$schema": "1.0.0",
        "artifactId": ontology["id"],
        "workspaceId": WORKSPACE,
        "displayName": ONTOLOGY,
        "type": DS_TYPE,
        "userDescription": "AEMO National Electricity Market: units (DUIDs), stations, "
                           "participants and their corporate ownership, regions, "
                           "interconnectors, and 5-minute generation and price "
                           "observations.",
    }),
]

existing = find_agent(AGENT)

if existing:
    agent_id = existing["id"]
    # No ?updateMetadata=true here, unlike the ontology scripts: that flag requires a
    # .platform part, and the Data Agent definition has no such part. Display name and
    # description are item metadata and are not being changed anyway.
    call("POST", f"{API}/workspaces/{WORKSPACE}/dataAgents/{agent_id}/updateDefinition",
         json={"definition": {"parts": parts}})
    print(f"Updated data agent '{AGENT}' ({agent_id})")
else:
    folder_id = next((f["id"] for f in
                      call("GET", f"{API}/workspaces/{WORKSPACE}/folders").json()["value"]
                      if f["displayName"] == FOLDER), None)
    created = call("POST", f"{API}/workspaces/{WORKSPACE}/dataAgents", json={
        "displayName": AGENT,
        "description": f"NEM question answering grounded on the {ONTOLOGY} ontology",
        "folderId": folder_id,
        "definition": {"parts": parts},
    }).json()
    agent_id = created.get("id") or find_agent(AGENT)["id"]
    print(f"Created data agent '{AGENT}' ({agent_id})")
    # folderId on create is not honoured by every preview item type, so assert placement.
    if folder_id:
        moved = session.post(f"{API}/workspaces/{WORKSPACE}/items/{agent_id}/move",
                             json={"targetFolderId": folder_id})
        print(f"  agent -> folder '{FOLDER}'" if moved.ok
              else f"  agent move -> {moved.status_code}: {moved.text[:160]}")

# Publishing is what makes the MCP endpoint work. Without it ask.py cannot connect.
call("POST", f"{API}/workspaces/{WORKSPACE}/dataAgents/{agent_id}/staging/publish",
     json={"publishedDescription": f"Grounded on {ONTOLOGY}"})
print("Published staging -> live")

# Echo back what Fabric actually stored, so a rejected or rewritten DS_TYPE is visible now
# rather than as a mystery empty answer later.
for p in get_definition(agent_id):
    if p["path"].endswith("datasource.json"):
        print(f"\nstored {p['path']}:")
        print(base64.b64decode(p["payload"]).decode())

print(f"\nMCP endpoint for ask.py:\n"
      f"  {API}/mcp/workspaces/{WORKSPACE}/dataagents/{agent_id}/agent")
