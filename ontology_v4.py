# aemo_nem_v4: fct_summary bound as an ENTITY at full 5-minute grain -- the design v3
# should have offered. Observation nodes keyed [DUID, DateKey, Interval] (three-part
# composite, never tested before v4), with MW/Price as PLAIN Double properties -- which,
# unlike v3's TimeSeries properties, ARE visible to GQL: sum(o.MW) works, at the fact
# table's own grain, with no aggregate table and no minted surrogate key.
#
# What it costs: ~10.5M Observation nodes + ~10.5M PRODUCED edges re-ingested on every
# refresh. Whether that ingests in minutes, hours, or not at all is exactly what this
# item exists to measure. The old "a fact table would wreck the graph" belief was based
# on a phantom 140M row count; the real table is 10.5M.
#
# Key-type rule (the reason DateKey exists): entity key parts may only be String or
# Integer. DUID (varchar) and time (int) qualify as-is; date (DATE) is banned, so
# fct_summary carries CAST(date AS VARCHAR) AS DateKey. The `time` column binds to a
# property named Interval because TIME is a GQL reserved-word risk.

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
ONTOLOGY  = "aemo_nem_v4"
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
        # No row filter is possible at bind time, so EnergyConnect (InService = false)
        # becomes a node like any other. Filter on InService in any reachability query.
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
            "StationName":             ("StationName",            "String"),
            "Region":                  ("Region",                 "String"),
            "UnitCount":               ("UnitCount",              "BigInt"),
            "GeneratingUnitCount":     ("GeneratingUnitCount",    "BigInt"),
            "LoadUnitCount":           ("LoadUnitCount",          "BigInt"),
            "BidirectionalUnitCount":  ("BidirectionalUnitCount", "BigInt"),
            "RegCapMW":                ("RegCapMW",               "Double"),
        },
    },
    "Unit": {
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
    # One node per fct_summary row: the fact at its own grain, measures GQL-queryable.
    "Observation": {
        "table": "fct_summary",
        "keys": ["DUID", "DateKey", "Interval"],
        "display": "DUID",
        "properties": {
            "DUID":     ("DUID",    "String"),
            "DateKey":  ("DateKey", "String"),
            "Interval": ("time",    "BigInt"),
            "MW":       ("mw",      "Double"),
            "Price":    ("price",   "Double"),
        },
    },
}

# (name, source entity, target entity, mapping table, source bindings, target bindings).
# PRODUCED's target binds all three key columns of the Observation composite key.
RELATIONSHIPS = [
    ("SUBSIDIARY_OF", "Participant",    "Participant", "dim_participant_parent",
     [("Participant", "Participant")],           [("ParentParticipant", "Participant")]),
    ("OPERATED_BY",   "Unit",           "Participant", "dim_duid",
     [("DUID", "DUID")],                         [("Participant", "Participant")]),
    ("PART_OF",       "Unit",           "Station",     "dim_duid",
     [("DUID", "DUID")],                         [("StationName", "StationName")]),
    ("OWNED_BY",      "Station",        "Participant", "dim_station",
     [("StationName", "StationName")],           [("Participant", "Participant")]),
    ("LOCATED_IN",    "Station",        "Region",      "dim_station",
     [("StationName", "StationName")],           [("Region", "RegionID")]),
    ("CONNECTS_FROM", "Interconnector", "Region",      "dim_interconnector",
     [("InterconnectorID", "InterconnectorID")], [("FromRegion", "RegionID")]),
    ("CONNECTS_TO",   "Interconnector", "Region",      "dim_interconnector",
     [("InterconnectorID", "InterconnectorID")], [("ToRegion", "RegionID")]),
    ("PRODUCED",      "Unit",           "Observation", "fct_summary",
     [("DUID", "DUID")],
     [("DUID", "DUID"), ("DateKey", "DateKey"), ("time", "Interval")]),
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
    resp.raise_for_status()
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
        "description": "NEM ontology v4: fct_summary as Observation nodes, 3-part composite key",
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
print(f"Refresh triggered on {graph['displayName']} (10.5M nodes + 10.5M edges: expect a LONG load)")
