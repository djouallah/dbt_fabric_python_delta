# aemo_nem_v5: the first ontology built on the WIDE mart layer.
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
# Unchanged rules that still bite: entity key parts may only be String or Integer (so dates
# are ISO strings via DateKey), and a Double property must be bound to a DOUBLE column --
# a parameterised DECIMAL(p,s) silently maps to String and ingests as all NULL.
#
# NOT bound, deliberately: fct_unit_dispatch (26.8M rows -- would triple the load for
# offered-capacity detail better served by SQL) and fct_constraint (intraday-only, tiny,
# and constraint IDs are opaque strings a graph adds nothing to).

import base64
import hashlib
import json
import time
import uuid

import requests
import duckrun
from duckrun.auth import get_fabric_token

WORKSPACE = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
LAKEHOUSE = "data"
SCHEMA    = "mart"
ONTOLOGY  = "aemo_nem_v5"
API       = "https://api.fabric.microsoft.com/v1"

# propertyName: (sourceColumn, valueType). "keys" is a list (composite keys allowed).
ENTITIES = {
    "Region": {
        "table": "dim_region",
        "keys": ["RegionID"],
        "properties": {
            "RegionID": ("RegionID", "String"),
            "State":    ("State",    "String"),
            "Market":   ("Market",   "String"),
        },
    },
    "Interconnector": {
        "table": "dim_interconnector",
        "keys": ["InterconnectorID"],
        # No row filter at bind time, so EnergyConnect (InService = false) is a node like any
        # other. Filter on InService in any reachability question.
        "properties": {
            "InterconnectorID": ("InterconnectorID", "String"),
            "Name":             ("Name",             "String"),
            "AcDc":             ("AcDc",             "String"),
            "InService":        ("InService",        "Boolean"),
        },
    },
    "Participant": {
        "table": "dim_participant",
        "keys": ["Participant"],
        "properties": {
            "Participant":             ("Participant",             "String"),
            "ParentParticipant":       ("ParentParticipant",       "String"),
            "IsCurated":               ("IsCurated",               "Boolean"),
            "IsRegisteredParticipant": ("IsRegisteredParticipant", "Boolean"),
        },
    },
    "Station": {
        "table": "dim_station",
        "keys": ["StationName"],
        "properties": {
            "StationName":            ("StationName",            "String"),
            "Region":                 ("Region",                 "String"),
            "UnitCount":              ("UnitCount",              "BigInt"),
            "GeneratingUnitCount":    ("GeneratingUnitCount",    "BigInt"),
            "LoadUnitCount":          ("LoadUnitCount",          "BigInt"),
            "BidirectionalUnitCount": ("BidirectionalUnitCount", "BigInt"),
            "RegCapMW":               ("RegCapMW",               "Double"),
        },
    },
    # Renamed from v4's `Unit`, which is a GQL reserved word.
    "GeneratingUnit": {
        "table": "dim_duid",
        "keys": ["DUID"],
        "properties": {
            "DUID":         ("DUID",                 "String"),
            "Region":       ("Region",               "String"),
            "FuelSource":   ("FuelSourceDescriptor", "String"),
            "DispatchType": ("DispatchType",         "String"),
            "Technology":   ("TechnologyType",       "String"),
            "RegCapMW":     ("RegCapMW",             "Double"),
            "StationName":  ("StationName",          "String"),
        },
    },
    # One node per fct_summary row: unit output and the regional price it faced.
    "Observation": {
        "table": "fct_summary",
        "keys": ["DUID", "DateKey", "TimeHHMM"],
        "display": "DUID",
        "properties": {
            "DUID":     ("DUID",     "String"),
            "DateKey":  ("DateKey",  "String"),
            "TimeHHMM": ("time",     "BigInt"),
            "RegionID": ("RegionID", "String"),
            "MW":       ("mw",       "Double"),
            "Price":    ("price",    "Double"),
        },
    },
    # NEW in v5. The regional truth: one row per region per interval. Price here is THE
    # regional spot price -- one value, not the same number copied onto 60 units.
    "RegionInterval": {
        "table": "fct_region",
        "keys": ["RegionID", "DateKey", "TimeHHMM"],
        "display": "RegionID",
        "properties": {
            "RegionID":               ("RegionID",               "String"),
            "DateKey":                ("DateKey",                "String"),
            "TimeHHMM":               ("time",                   "BigInt"),
            "Price":                  ("price",                  "Double"),
            "TotalDemand":            ("TotalDemand",            "Double"),
            "NetInterchange":         ("NetInterchange",         "Double"),
            "AvailableGeneration":    ("AvailableGeneration",    "Double"),
            "DispatchableGeneration": ("DispatchableGeneration", "Double"),
        },
    },
    # NEW in v5. Power moving between regions, full history. Region-PAIR grain: QNI and
    # Terranora share the QLD1-NSW1 corridor and cannot be separated from net injections.
    "Flow": {
        "table": "fct_interconnector_derived",
        "keys": ["LinkID", "DateKey", "TimeHHMM"],
        "display": "LinkID",
        "properties": {
            "LinkID":        ("LinkID",        "String"),
            "DateKey":       ("DateKey",       "String"),
            "TimeHHMM":      ("time",          "BigInt"),
            "LinkName":      ("LinkName",      "String"),
            "FromRegion":    ("FromRegion",    "String"),
            "ToRegion":      ("ToRegion",      "String"),
            "FlowMW":        ("FlowMW",        "Double"),
            "NetworkLossMW": ("NetworkLossMW", "Double"),
        },
    },
}

# (name, source entity, target entity, mapping table, source bindings, target bindings).
RELATIONSHIPS = [
    ("SUBSIDIARY_OF", "Participant",    "Participant", "dim_participant_parent",
     [("Participant", "Participant")],           [("ParentParticipant", "Participant")]),
    ("OPERATED_BY",   "GeneratingUnit", "Participant", "dim_duid",
     [("DUID", "DUID")],                         [("Participant", "Participant")]),
    ("PART_OF",       "GeneratingUnit", "Station",     "dim_duid",
     [("DUID", "DUID")],                         [("StationName", "StationName")]),
    ("OWNED_BY",      "Station",        "Participant", "dim_station",
     [("StationName", "StationName")],           [("Participant", "Participant")]),
    ("LOCATED_IN",    "Station",        "Region",      "dim_station",
     [("StationName", "StationName")],           [("Region", "RegionID")]),
    ("CONNECTS_FROM", "Interconnector", "Region",      "dim_interconnector",
     [("InterconnectorID", "InterconnectorID")], [("FromRegion", "RegionID")]),
    ("CONNECTS_TO",   "Interconnector", "Region",      "dim_interconnector",
     [("InterconnectorID", "InterconnectorID")], [("ToRegion", "RegionID")]),
    ("PRODUCED",      "GeneratingUnit", "Observation", "fct_summary",
     [("DUID", "DUID")],
     [("DUID", "DUID"), ("DateKey", "DateKey"), ("time", "TimeHHMM")]),
    # NEW: a region's own dispatch outcome, interval by interval.
    ("OBSERVED",      "Region",         "RegionInterval", "fct_region",
     [("RegionID", "RegionID")],
     [("RegionID", "RegionID"), ("DateKey", "DateKey"), ("time", "TimeHHMM")]),
    # NEW: both ends of each corridor flow, so reachability and transfer questions traverse.
    ("FLOW_FROM",     "Flow",           "Region",      "fct_interconnector_derived",
     [("LinkID", "LinkID"), ("DateKey", "DateKey"), ("time", "TimeHHMM")],
     [("FromRegion", "RegionID")]),
    ("FLOW_TO",       "Flow",           "Region",      "fct_interconnector_derived",
     [("LinkID", "LinkID"), ("DateKey", "DateKey"), ("time", "TimeHHMM")],
     [("ToRegion", "RegionID")]),
]


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
lh = next(l for l in ws.list_lakehouses() if l["displayName"] == LAKEHOUSE)
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

for name, src, tgt, table, src_bind, tgt_bind in RELATIONSHIPS:
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


existing = next((o for o in call("GET", f"{API}/workspaces/{WORKSPACE}/ontologies").json()["value"]
                 if o["displayName"] == ONTOLOGY), None)

if existing:
    call("POST", f"{API}/workspaces/{WORKSPACE}/ontologies/{existing['id']}/updateDefinition"
                 "?updateMetadata=true", json={"definition": {"parts": parts}})
    print(f"Updated ontology '{ONTOLOGY}' ({existing['id']})")
else:
    created = call("POST", f"{API}/workspaces/{WORKSPACE}/ontologies", json={
        "displayName": ONTOLOGY,
        "description": "NEM ontology v5: unit observations plus regional demand/price and "
                       "interconnector flows, on the wide mart layer",
        "definition": {"parts": parts},
    })
    print(f"Created ontology '{ONTOLOGY}' ({created.json().get('id', '')})")

print(f"{len(ENTITIES)} entity types, {len(RELATIONSHIPS)} relationship types, {len(parts)} parts")

# A schema change re-ingests automatically, but changed DATA in the bound tables does not.
# Trigger a refresh explicitly so a redeploy after `dbt run` is never serving stale rows.
graph = next(m for m in call("GET", f"{API}/workspaces/{WORKSPACE}/GraphModels").json()["value"]
             if ONTOLOGY in m["displayName"])
session.post(f"{API}/workspaces/{WORKSPACE}/items/{graph['id']}/jobs/instances"
             "?jobType=RefreshGraph").raise_for_status()
print(f"Refresh triggered on {graph['displayName']} "
      f"(~12.5M nodes: 11.9M Observation + 373k RegionInterval + 298k Flow — expect a LONG load)")
