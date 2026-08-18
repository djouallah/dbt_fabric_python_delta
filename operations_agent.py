# aemo_nem_ops: a Fabric Operations Agent grounded on the aemo_nem Ontology -- the
# OPERATIONAL side of the ontology. The service compiles INSTRUCTIONS below into a playbook
# of rules, evaluates each rule's graph query every ~5 minutes, and messages the creator in
# Teams when one fires. The RefreshAemoData action lets a recipient approve a run of the
# run_pipeline Data Pipeline straight from the Teams card (the agent acts on behalf of its
# CREATOR, via OBO -- approving is not the same as running it yourself).
#
# NOT wired into deploy.py, deliberately: the OperationsAgent REST API accepts USER
# identities only -- service principals and managed identities are rejected -- so CI's OIDC
# app identity cannot call it. Run from a laptop under `az login`:
#     python operations_agent.py            # create-or-update, started
#     python operations_agent.py --dump     # print the stored definition, decoded
#
# STATUS 2026-08-18: create answers 403 FeatureNotAvailable, and the cause is the CAPACITY
# REGION — the workspace's P1 is in East US, one of exactly two US regions where
# "Operations agent (preview)" is excluded (learn.microsoft.com/fabric/admin/
# region-availability). Tenant settings were eliminated first and are all correctly on.
# The tenant's West Europe P1 has no exclusions; an agent in a West Europe workspace
# pointing its dataSources entry cross-workspace at this ontology is the schema-supported
# escape hatch (each dataSource carries its own workspaceId). Re-run unchanged if the
# East US exclusion lifts.
#
# The definition is ONE part, Configurations.json (the documented OperationsAgentV1 format):
# {configuration: {instructions, dataSources, actions, messageDestination?}, playbook,
# shouldRun}. playbook is sent as {} -- the service generates it from the instructions.
# messageDestination is omitted -> Teams DM to the creator (needs the "Fabric Operations
# Agent" Teams app installed). Prereqs beyond that are tenant-level: the operations agent
# preview switch, Copilot, Azure OpenAI, and cross-geo AI processing when the capacity is
# not in a US/EU region. Trial capacities are not supported.

import base64
import json
import sys
import time
import uuid

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
ONTOLOGY  = "aemo_nem"
AGENT     = "aemo_nem_ops"
PIPELINE  = "run_pipeline"   # the FabricJobAction target
FOLDER    = "aemo"           # workspace folder the agent lives in
API       = "https://api.fabric.microsoft.com/v1"

# dataSources/actions entries carry GUID ids. Minting them from names keeps re-runs from
# rewriting identity on every updateDefinition -- the same reason ontology.py hashes names.
NS = uuid.uuid5(uuid.NAMESPACE_URL, "fabric/operationsAgents/aemo_nem_ops")

# Concise on purpose: the service compiles its own playbook from this, so it needs the data
# shape and the watch list, not the data agent's GQL dialect essay. Thresholds live here --
# to tune a rule, edit and re-run this script.
INSTRUCTIONS = """
You monitor the Australian National Electricity Market (NEM) through the aemo_nem ontology.

Data shape:
- RegionInterval: one row per region per 5-minute interval. Properties: RegionID (NSW1,
  QLD1, SA1, TAS1, VIC1), DateKey, TimeHHMM, Price ($/MWh), TotalDemand (MW),
  NetInterchange (MW, positive when the region is exporting), AvailableGeneration (MW),
  DispatchableGeneration (MW), LorSurplus (MW), MarketSuspendedFlag.
- Flow: one row per interconnector corridor per 5-minute interval. Properties: LinkID
  ('QLD1-NSW1', 'VIC1-NSW1', 'SA1-VIC1', 'TAS1-VIC1'), LinkName, FromRegionID, ToRegionID,
  FlowMW (positive in the From -> To direction), NetworkLossMW.
- DateKey is a YYYYMMDD integer and TimeHHMM is clock time encoded as HHMM (0, 5, ... 55,
  100, ... 2355 -- nothing exists between 55 and 100). "Latest" means the maximum DateKey,
  then the maximum TimeHHMM within that day. New intervals arrive in batches roughly every
  12 hours, so the newest data being a few hours old is normal.

Watch the latest data for these conditions:
1. Price spike: any region with Price above 300 $/MWh.
2. Negative price event: any region with Price below -100 $/MWh.
3. Lack of reserve: any region with LorSurplus below 0.
4. Market suspension: any region with MarketSuspendedFlag not equal to 0.
5. Basslink at its limit: LinkID 'TAS1-VIC1' with FlowMW above 470 or below -470 (the link
   is rated 478 MW).
6. Stale data: if the latest interval is more than 13 hours old, recommend the
   RefreshAemoData action to pull the latest AEMO files.

In every alert, state the region or link, the DateKey and TimeHHMM of the triggering
interval, and the measured value.
""".strip()

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
    if not resp.ok:
        raise RuntimeError(f"{method} {url} -> {resp.status_code}: {resp.text[:600]}")
    return resp


def part(path, obj):
    payload = base64.b64encode(json.dumps(obj, indent=2).encode()).decode()
    return {"path": path, "payload": payload, "payloadType": "InlineBase64"}


def find_agent(name):
    return next((a for a in
                 call("GET", f"{API}/workspaces/{WORKSPACE}/operationsAgents").json()["value"]
                 if a["displayName"] == name), None)


def get_definition(agent_id):
    """getDefinition is an LRO whose payload lives at {Location}/result, not in the
    operation-status body that call() would hand back."""
    resp = session.post(
        f"{API}/workspaces/{WORKSPACE}/operationsAgents/{agent_id}/getDefinition")
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
                 call("GET", f"{API}/workspaces/{WORKSPACE}/operationsAgents").json()["value"]]
        raise SystemExit(f"No operations agent named '{target}'. Found: {names}")
    print(f"{target} ({found['id']})\n")
    for p in get_definition(found["id"]):
        print("=" * 70)
        print(p["path"])
        print(base64.b64decode(p["payload"]).decode())
    raise SystemExit(0)

ontologies = call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
ontology = next((o for o in ontologies if o["displayName"] == ONTOLOGY), None)
if not ontology:
    raise SystemExit(f"No ontology named '{ONTOLOGY}' in the workspace — run ontology.py "
                     f"first. Found: {[o['displayName'] for o in ontologies] or 'none'}")

# The pipeline shows up in the plain item list (unlike Ontology/GraphModel/DataAgent).
pipelines = call("GET", f"{API}/workspaces/{WORKSPACE}/items?type=DataPipeline").json()["value"]
pipeline = next((p for p in pipelines if p["displayName"] == PIPELINE), None)
if not pipeline:
    raise SystemExit(f"No data pipeline named '{PIPELINE}' — deploy the Fabric items first "
                     f"(deploy=no_model). Found: {[p['displayName'] for p in pipelines] or 'none'}")

parts = [part("Configurations.json", {
    "configuration": {
        "instructions": INSTRUCTIONS,
        "dataSources": {
            "aemo": {"id": ontology["id"], "type": "Ontology", "workspaceId": WORKSPACE},
        },
        "actions": {
            "refreshAemoData": {
                "id": str(uuid.uuid5(NS, "action/refreshAemoData")),
                "kind": "FabricJobAction",
                "displayName": "RefreshAemoData",
                "description": "Run the run_pipeline data pipeline to download the latest "
                               "AEMO files and rebuild the mart tables",
                "connection": {"jobArtifactId": pipeline["id"],
                               "jobWorkspaceId": WORKSPACE,
                               "itemType": "DataPipeline",
                               "jobType": "Pipeline"},
            },
        },
        # messageDestination omitted -> Teams DM to the creator.
    },
    "playbook": {},
    "shouldRun": True,
})]

existing = find_agent(AGENT)

if existing:
    agent_id = existing["id"]
    # No ?updateMetadata=true: no .platform part is sent, same as the data agent.
    call("POST", f"{API}/workspaces/{WORKSPACE}/operationsAgents/{agent_id}/updateDefinition",
         json={"definition": {"parts": parts}})
    print(f"Updated operations agent '{AGENT}' ({agent_id})")
else:
    folder_id = next((f["id"] for f in
                      call("GET", f"{API}/workspaces/{WORKSPACE}/folders").json()["value"]
                      if f["displayName"] == FOLDER), None)
    created = call("POST", f"{API}/workspaces/{WORKSPACE}/operationsAgents", json={
        "displayName": AGENT,
        "description": f"Operational monitoring of the NEM grounded on the {ONTOLOGY} ontology",
        "folderId": folder_id,
        "definition": {"parts": parts},
    }).json()
    agent_id = created.get("id") or find_agent(AGENT)["id"]
    print(f"Created operations agent '{AGENT}' ({agent_id})")
    # folderId on create is not honoured by every preview item type, so assert placement.
    if folder_id:
        moved = session.post(f"{API}/workspaces/{WORKSPACE}/items/{agent_id}/move",
                             json={"targetFolderId": folder_id})
        print(f"  agent -> folder '{FOLDER}'" if moved.ok
              else f"  agent move -> {moved.status_code}: {moved.text[:160]}")

# Echo what Fabric actually stored: a rejected/rewritten field (the "Ontology" dataSource
# type, the action shape, whether shouldRun stuck) is visible now rather than as a silent
# no-op later. If the playbook stays empty in the portal, hit "Generate playbook" once.
for p in get_definition(agent_id):
    print(f"\nstored {p['path']}:")
    print(base64.b64decode(p["payload"]).decode())
