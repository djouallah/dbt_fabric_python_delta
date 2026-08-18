# aemo_nem_ops: a Fabric Operations Agent grounded on the aemo_nem Ontology -- the
# OPERATIONAL side of the ontology. The service compiles the instructions into a playbook
# of rules, evaluates each rule's graph query every ~5 minutes, and messages the creator in
# Teams when one fires.
#
# ============================ FINAL VERDICT 2026-08-18 =============================
# THE AGENT MUST BE CREATED AND MANAGED IN THE PORTAL. The public definition API and the
# portal experience are two DISCONNECTED stores in this preview:
#   - An agent created here, with a schema-valid definition the echo confirmed stored,
#     answers 400 "Unable to start playbook generation" in the portal -- the generator
#     reads an internal store where the agent is blank and unprovisioned (the portal
#     create flow also provisions the Entra Agent ID; this API does not).
#   - The reverse proof: a PORTAL-created agent that generates playbooks fine dumps an
#     EMPTY public definition (instructions "", dataSources {}, shouldRun false). The
#     portal neither reads nor writes the public parts.
# So the advertised CI/CD path is not wired up yet. This script's remaining jobs:
#   python operations_agent.py --dump [name]   # inspect any agent's public definition
#   INSTRUCTIONS below                          # the version-controlled text to PASTE
#                                               # into the portal agent's instructions
# The deploy path is kept for when Microsoft connects the two sides, but it is gated
# behind --force-deploy because today it produces an agent the portal cannot use.
# ===================================================================================
#
# NOT wired into deploy.py, deliberately: the OperationsAgent REST API accepts USER
# identities only -- service principals and managed identities are rejected -- so CI's OIDC
# app identity cannot call it. Run from a laptop under `az login`:
#     python operations_agent.py            # create-or-update, started
#     python operations_agent.py --dump     # print the stored definition, decoded
#
# Everything lives in ONE workspace now (plan B): the whole stack deploys to a workspace
# whose capacity region supports the item type. The original workspace's P1 sits in East
# US — one of exactly two US regions where "Operations agent (preview)" is excluded
# (learn.microsoft.com/fabric/admin/region-availability) — and answers 403
# FeatureNotAvailable on create; tenant settings were eliminated first, all correctly on.
# A cross-workspace dataSource DID work (each entry carries its own workspaceId), so a
# split is possible if ever needed again; co-location just removes the unproven
# cross-region agent->graph hop.
#
# The definition is ONE part, Configurations.json -- MEASURED against the live service,
# which differs from the docs article in three ways (all verified 2026-08-18 by pushing
# incremental definitions at a bare agent and reading its stored skeleton back):
#   - the part carries a "$schema" field (developer.microsoft.com/json-schemas/fabric/
#     item/operationsAgents/definition/1.0.0/schema.json);
#   - there is NO "playbook" key, despite the article calling it required -- the service's
#     own skeleton omits it and every successful push here omitted it;
#   - a FabricJobAction ALWAYS fails with 400 UnknownError through this API path -- every
#     schema-valid variant (same-workspace and cross-, Pipeline and RunNotebook, with and
#     without parameters), while a PowerAutomateAction in the same slot returns 200. The
#     job action needs registration the definition-import path evidently can't do yet, so
#     INCLUDE_JOB_ACTION below stays False and the agent ships Teams-alert-only (the
#     default Teams DM to the creator needs no action config at all).
# The CROSS-WORKSPACE ontology dataSource WORKS: agent in one workspace, ontology in
# another, accepted with a plain workspaceId on the dataSources entry.
# Three more measured behaviours:
#   - getDefinition SCRUBS GUIDs on read: the stored dataSource id and .platform logicalId
#     echo back as 00000000-... . The write DID validate and store the real id (pushing the
#     zeros back fails with 404 EntityNotFound, which proves ids resolve on write). So the
#     zeroed echo is fine, and a --dump is NOT round-trippable -- always deploy from this
#     script, never from a dump.
#   - "shouldRun": true is COERCED TO FALSE on import. Starting the agent needs its
#     playbook, and generating that is a portal operation: open the agent, Generate
#     playbook, review, Start. One-time, and again after instruction changes if the
#     playbook should follow them.
#   - PATCH /operationsAgents/{id} updates displayName/description normally.
# messageDestination is omitted -> Teams DM to the creator (needs the "Fabric Operations
# Agent" Teams app installed). If an action is ever added in the PORTAL, --dump and merge
# it here BEFORE re-running this script -- updateDefinition replaces the whole definition
# and would silently wipe it.

import base64
import json
import os
import sys
import time
import uuid

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = os.environ.get("WS_ID", "450bf196-431f-463f-9316-2d1ce1da98db")  # sqlengines
ONTOLOGY  = "aemo_nem"
AGENT     = "aemo_nem_ops"
PIPELINE  = "run_pipeline"   # the FabricJobAction target, when the flag below turns on
FOLDER    = os.environ.get("FOLDER", "FabricIQ")
API       = "https://api.fabric.microsoft.com/v1"
SCHEMA    = ("https://developer.microsoft.com/json-schemas/fabric/item/operationsAgents/"
             "definition/1.0.0/schema.json")

# Flip to True and re-run once Fabric's definition path stops 400ing on FabricJobAction
# (re-test occasionally; it is a service-side gap, not a payload problem -- see header).
INCLUDE_JOB_ACTION = False

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
6. Stale data: if the latest interval is more than 13 hours old, alert that the feed is
   stale -- the pipeline normally lands new data roughly every 12 hours.

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

if "--force-deploy" not in sys.argv:
    raise SystemExit(
        "Refusing to deploy: the OperationsAgent public definition API is disconnected "
        "from the portal experience (see the header) — an agent created here cannot "
        "generate a playbook. Create/manage the agent in the portal and paste "
        "INSTRUCTIONS from this file. Use --dump to inspect, or --force-deploy to "
        "override once Microsoft wires the two sides together.")

ontologies = call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
ontology = next((o for o in ontologies if o["displayName"] == ONTOLOGY), None)
if not ontology:
    raise SystemExit(f"No ontology named '{ONTOLOGY}' in the workspace — run ontology.py "
                     f"first. Found: {[o['displayName'] for o in ontologies] or 'none'}")

actions = {}
if INCLUDE_JOB_ACTION:
    # The pipeline shows up in the plain item list (unlike Ontology/GraphModel/DataAgent).
    pipelines = call("GET", f"{API}/workspaces/{WORKSPACE}/items?type=DataPipeline").json()["value"]
    pipeline = next((p for p in pipelines if p["displayName"] == PIPELINE), None)
    if not pipeline:
        raise SystemExit(f"No data pipeline named '{PIPELINE}' — deploy the Fabric items "
                         f"first (deploy=no_model). "
                         f"Found: {[p['displayName'] for p in pipelines] or 'none'}")
    actions["refreshAemoData"] = {
        "id": str(uuid.uuid5(NS, "action/refreshAemoData")),
        "kind": "FabricJobAction",
        "displayName": "RefreshAemoData",
        "description": "Run the run_pipeline data pipeline to download the latest "
                       "AEMO files and rebuild the mart tables",
        "connection": {"jobArtifactId": pipeline["id"],
                       "jobWorkspaceId": WORKSPACE,
                       "itemType": "DataPipeline",
                       "jobType": "Pipeline"},
    }

# No "playbook" key: the service's own skeleton has none and pushes that include one fail
# once the definition is non-trivial. messageDestination omitted -> Teams DM to the creator.
parts = [part("Configurations.json", {
    "$schema": SCHEMA,
    "configuration": {
        "instructions": INSTRUCTIONS,
        "dataSources": {
            "aemo": {"id": ontology["id"], "type": "Ontology", "workspaceId": WORKSPACE},
        },
        "actions": actions,
    },
    "shouldRun": True,
})]

existing = find_agent(AGENT)
folder_id = next((f["id"] for f in
                  call("GET", f"{API}/workspaces/{WORKSPACE}/folders").json()["value"]
                  if f["displayName"] == FOLDER), None)

if existing:
    agent_id = existing["id"]
    # No ?updateMetadata=true: no .platform part is sent, same as the data agent.
    call("POST", f"{API}/workspaces/{WORKSPACE}/operationsAgents/{agent_id}/updateDefinition",
         json={"definition": {"parts": parts}})
    print(f"Updated operations agent '{AGENT}' ({agent_id})")
else:
    created = call("POST", f"{API}/workspaces/{WORKSPACE}/operationsAgents", json={
        "displayName": AGENT,
        "description": f"Operational monitoring of the NEM grounded on the {ONTOLOGY} ontology",
        "folderId": folder_id,
        "definition": {"parts": parts},
    }).json()
    agent_id = created.get("id") or find_agent(AGENT)["id"]
    print(f"Created operations agent '{AGENT}' ({agent_id})")

# Assert placement on BOTH paths: folderId on create is not honoured by every preview item
# type, and an agent can predate the folder (this one did — created at the workspace root
# before the FabricIQ deploy existed). Moving an item already in place is harmless.
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
