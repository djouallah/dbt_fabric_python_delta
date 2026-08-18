# aemo_nem: DEPLOY LOGIC for the ontology. The model itself is data, and lives in
# ontology.yaml -- entities, their properties and bindings, and the relationships between
# them. Nothing about the shape of the ontology is decided in this file; it turns that YAML
# into Fabric definition parts and pushes them.
#
# `python ontology.py --check` builds the parts and diffs them against a folder previously
# fetched by download.py, printing what would change, without deploying anything. That is the
# proof a YAML edit does what you meant. (argv, like data_agent.py's --dump: deploy.py runs
# this under runpy with ITS argv, and deploy.py takes no arguments.)
#
# v4 proved a leaf-grain fact table works in the graph (11.28M Observation nodes, exact
# counts, ~5 min load). What it could not do was answer a regional question honestly:
# fct_summary carried only unit MW and the regional price copied onto every unit, so
# "average spot price in SA1" had to be a unit-weighted average over ~15k readings, and the
# data agent produced 88x fan-outs trying to compute it. There was no demand anywhere.
#
# The mart layer now exposes what AEMO always published, so v5 adds two small entity sets
# that carry the regional truth directly:
#
#   RegionInterval  373k nodes  <- fct_region: TotalDemand, NetInterchange,
#                                 AvailableGeneration, and the ONE authoritative Price per
#                                 region per interval. No unit weighting, no fan-out.
#   Flow            298k nodes  <- fct_interconnector_derived: power actually moving between
#                                 regions, full history.
#
# Both are ~600k nodes on top of v4's 11.3M, so the load cost is nearly free while removing
# the class of question v4 got wrong.
#
# TWO RENAMES that remove GQL reserved-word traps rather than documenting them:
#   Unit     -> GeneratingUnit   (`Unit` is reserved; a bare (u:Unit) is a SYNTAX error)
#   Interval -> TimeHHMM         (`Interval` is reserved too, and the name was a lie --
#                                 the column is HHMM, 0..2355, not minutes past midnight)
# Nothing in v5 needs a backtick.
#
# The rules that shape ontology.yaml -- key parts are String/Integer only, measures must bind
# as Double -- are documented there, next to the data they constrain.

import base64
import hashlib
import json
import os
import sys
import time
import uuid

import requests
import duckrun
import yaml
from duckrun.auth import get_fabric_token

# Environment, not model: which workspace and lakehouse the ontology is deployed against.
# The model's own name and description come from the YAML. Env-driven with the same
# defaults as pipeline.yml / deploy.py, so CI inputs flow through and a bare run targets
# the same place.
WORKSPACE = os.environ.get("WS_ID", "450bf196-431f-463f-9316-2d1ce1da98db")  # sqlengines
LAKEHOUSE = os.environ.get("LH_NAME", "aemo")
SCHEMA    = "mart"
FOLDER    = os.environ.get("FOLDER", "FabricIQ")   # folder the ontology and graph live in
API       = "https://api.fabric.microsoft.com/v1"
MODEL_FILE = "ontology.yaml"
CHECK_DIR  = os.path.join("fabric_download", "aemo_nem.Ontology")   # --check compares here

os.chdir(os.path.dirname(os.path.abspath(__file__)))
with open(MODEL_FILE, encoding="utf-8") as f:
    MODEL = yaml.safe_load(f)

ONTOLOGY      = MODEL["name"]
DESCRIPTION   = MODEL["description"]
ENTITIES      = MODEL["entities"]
RELATIONSHIPS = MODEL["relationships"]



def big_id(*parts):
    """Deterministic positive 64-bit id, so re-running updates in place instead of
    creating duplicate types."""
    digest = hashlib.blake2b("::".join(parts).encode(), digest_size=8).digest()
    return str(int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF)


def guid(*parts):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "::".join(parts)))


def part(path, obj):
    payload = base64.b64encode(json.dumps(obj, indent=2).encode()).decode()
    return {"path": path, "payload": payload, "payloadType": "InlineBase64"}


ws = duckrun.workspace(WORKSPACE)
lakehouses = ws.list_lakehouses()
lh = next((l for l in lakehouses if l["displayName"] == LAKEHOUSE), None)
if not lh:
    # Explicit rather than a bare StopIteration: this is the normal state before the first
    # deploy, and the useful signal is "run deploy.py", not a traceback. The TRANSFORM
    # lakehouse is meant here -- the ontology binds mart tables, never the raw archive.
    raise SystemExit(f"lakehouse '{LAKEHOUSE}' not found — run deploy.py first. "
                     f"Found: {[l['displayName'] for l in lakehouses] or 'none'}")
source_table = {"sourceType": "LakehouseTable", "workspaceId": WORKSPACE,
                "itemId": lh["id"], "sourceSchema": SCHEMA}

parts = [
    part(".platform", {"metadata": {"type": "Ontology", "displayName": ONTOLOGY}}),
    part("definition.json", {}),
]

for entity, spec in ENTITIES.items():
    eid = big_id("entity", entity)
    display = spec.get("display", spec["keys"][0])
    parts.append(part(f"EntityTypes/{eid}/definition.json", {
        "id": eid,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": entity,
        "entityIdParts": [big_id("prop", entity, k) for k in spec["keys"]],
        "displayNamePropertyId": big_id("prop", entity, display),
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            {"id": big_id("prop", entity, name), "name": name,
             "redefines": None, "baseTypeNamespaceType": None, "valueType": vtype}
            for name, (_, vtype) in spec["properties"].items()
        ],
        "timeseriesProperties": [],
    }))
    binding_id = guid("binding", entity, spec["table"])
    parts.append(part(f"EntityTypes/{eid}/DataBindings/{binding_id}.json", {
        "id": binding_id,
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": column, "targetPropertyId": big_id("prop", entity, name)}
                for name, (column, _) in spec["properties"].items()
            ],
            "sourceTableProperties": {**source_table, "sourceTableName": spec["table"]},
        },
    }))

for rel in RELATIONSHIPS:
    name, src, tgt, table = rel["name"], rel["from"], rel["to"], rel["table"]
    src_bind, tgt_bind = rel["from_bind"], rel["to_bind"]
    rid = big_id("rel", name)
    parts.append(part(f"RelationshipTypes/{rid}/definition.json", {
        "id": rid,
        "namespace": "usertypes",
        "name": name,
        "namespaceType": "Custom",
        "source": {"entityTypeId": big_id("entity", src)},
        "target": {"entityTypeId": big_id("entity", tgt)},
    }))
    ctx_id = guid("ctx", name, table)
    parts.append(part(f"RelationshipTypes/{rid}/Contextualizations/{ctx_id}.json", {
        "id": ctx_id,
        "dataBindingTable": {**source_table, "sourceTableName": table},
        "sourceKeyRefBindings": [
            {"sourceColumnName": col, "targetPropertyId": big_id("prop", src, prop)}
            for col, prop in src_bind
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": col, "targetPropertyId": big_id("prop", tgt, prop)}
            for col, prop in tgt_bind
        ],
    }))

def sends_same(built, stored):
    """Whether everything we SEND already matches what Fabric STORED.

    Not equality: the service enriches what it keeps. Every stored part gains a `$schema`,
    `.platform` gains a `config` block with a logicalId, and an entity definition gains
    `untypedProperties`. Those are Fabric's, not ours, so a plain == reports all 40 parts as
    changed on an untouched model. Compare our keys only, and ignore extra ones."""
    if isinstance(built, dict):
        return isinstance(stored, dict) and all(
            k in stored and sends_same(v, stored[k]) for k, v in built.items())
    if isinstance(built, list):
        return (isinstance(stored, list) and len(built) == len(stored)
                and all(sends_same(a, b) for a, b in zip(built, stored)))
    return built == stored


if "--check" in sys.argv:
    # Diff the parts this YAML produces against a folder download.py fetched earlier, and
    # stop. No token, no deploy. An empty diff means the edit was a no-op on the wire; an
    # unexpected ADDED/REMOVED pair is the signature of an accidental RENAME, because every
    # id is hashed from a name -- a renamed entity does not update, it forks.
    built = {p["path"]: json.loads(base64.b64decode(p["payload"]).decode()) for p in parts}
    stored = {}
    for dirpath, _, filenames in os.walk(CHECK_DIR):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel_path = os.path.relpath(full, CHECK_DIR).replace(os.sep, "/")
            with open(full, encoding="utf-8-sig") as fh:      # Fabric writes a BOM
                stored[rel_path] = json.load(fh)
    if not stored:
        raise SystemExit(f"nothing to compare against in {CHECK_DIR!r} — "
                         f"run `python download.py` first")
    added, removed = sorted(set(built) - set(stored)), sorted(set(stored) - set(built))
    changed = sorted(p for p in set(built) & set(stored)
                     if not sends_same(built[p], stored[p]))
    for label, paths in (("ADDED", added), ("REMOVED", removed), ("CHANGED", changed)):
        for p in paths:
            print(f"  {label:<8} {p}")
    print(f"\n{len(built)} parts built, {len(stored)} stored — "
          f"{len(added)} added, {len(removed)} removed, {len(changed)} changed")
    raise SystemExit(1 if (added or removed or changed) else 0)

session = requests.Session()
session.headers["Authorization"] = f"Bearer {get_fabric_token()}"


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


folder_id = next((f["id"] for f in
                  call("GET", f"{API}/workspaces/{WORKSPACE}/folders").json()["value"]
                  if f["displayName"] == FOLDER), None)


def place(item_id, label):
    """Move an item into FOLDER if it isn't already there.

    `folderId` on create only works for items we create ourselves; the graph model is
    created by Fabric alongside the ontology and lands wherever Fabric puts it. An existing
    item updated in place also keeps its old location. So placement is asserted separately
    rather than assumed."""
    if not folder_id:
        return
    resp = session.post(f"{API}/workspaces/{WORKSPACE}/items/{item_id}/move",
                        json={"targetFolderId": folder_id})
    print(f"  {label} -> folder '{FOLDER}'"
          if resp.ok else f"  {label} move -> {resp.status_code}: {resp.text[:160]}")


existing = next((o for o in call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
                 if o["displayName"] == ONTOLOGY), None)

if existing:
    call("POST", f"{API}/workspaces/{WORKSPACE}/ontologies/{existing['id']}/updateDefinition"
                 "?updateMetadata=true", json={"definition": {"parts": parts}})
    ontology_id = existing["id"]
    print(f"Updated ontology '{ONTOLOGY}' ({ontology_id})")
else:
    created = call("POST", f"{API}/workspaces/{WORKSPACE}/ontologies", json={
        "displayName": ONTOLOGY,
        "description": DESCRIPTION,
        "folderId": folder_id,
        "definition": {"parts": parts},
    })
    ontology_id = created.json().get("id") or next(
        o["id"] for o in call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
        if o["displayName"] == ONTOLOGY)
    print(f"Created ontology '{ONTOLOGY}' ({ontology_id})")

place(ontology_id, "ontology")

print(f"{len(ENTITIES)} entity types, {len(RELATIONSHIPS)} relationship types, {len(parts)} parts")

# A schema change re-ingests automatically, but changed DATA in the bound tables does not.
# Trigger a refresh explicitly so a redeploy after `dbt run` is never serving stale rows.
# The graph model is created by Fabric, named "<ontology>_graph_<guid>". It is a CHILD of the
# ontology and inherits its folder -- moving it on its own is rejected with
# CannotMoveChildOnly, "The child item cannot be moved without its parent item". So placing
# the ontology places the graph too; do not try to move it separately.
graph = next(m for m in call("GET", f"{API}/workspaces/{WORKSPACE}/GraphModels").json()["value"]
             if m["displayName"].startswith(f"{ONTOLOGY}_graph"))
session.post(f"{API}/workspaces/{WORKSPACE}/items/{graph['id']}/jobs/instances"
             "?jobType=RefreshGraph").raise_for_status()
print(f"Refresh triggered on {graph['displayName']} "
      f"(~12.5M nodes: 11.9M Observation + 373k RegionInterval + 298k Flow — expect a LONG load)")
