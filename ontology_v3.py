# aemo_nem_v3: no aggregate tables at all. The dimensions bind as nodes exactly like v2
# (composite keys, unprefixed names), and fct_summary binds DIRECTLY to Unit as a
# TimeSeries data binding -- 10.5M observations become time-series properties (MW, Price)
# attached to ~750 Unit nodes, not nodes of their own. No UnitDay entity, no PRODUCED
# edge, no minted keys, no reification: the graph holds structure, the time series holds
# the measures, both on the same entity.
#
# What fct_summary needed to become bindable (see the model):
#   - mw/price cast DECIMAL(18,4) -> DOUBLE (parameterised decimals bind as String and
#     ingest NULL against Double properties)
#   - a single `ts` TIMESTAMP column (the split date + time(HHMM) pair cannot be chosen
#     as a binding timestamp)
#
# The TimeSeries binding must also bind a column to the entity's KEY property (DUID) so
# each observation row can be attached to its Unit instance.

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
ONTOLOGY  = "aemo_nem_v3"
API       = "https://api.fabric.microsoft.com/v1"

# propertyName: (sourceColumn, valueType). "keys" is a list (composite keys allowed).
# "timeseries" declares time-series properties bound from "ts_table" keyed on "ts_column".
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
        "timeseries": {
            "Timestamp": ("ts",    "DateTime"),
            "MW":        ("mw",    "Double"),
            "Price":     ("price", "Double"),
        },
        "ts_table":  "fct_summary",
        "ts_column": "ts",
    },
}

# (name, source entity, target entity, mapping table, source bindings, target bindings).
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
    ts_props = spec.get("timeseries", {})
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
        "timeseriesProperties": [
            {"id": big_id("prop", entity, name), "name": name,
             "redefines": None, "baseTypeNamespaceType": None, "valueType": vtype}
            for name, (_, vtype) in ts_props.items()
        ],
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
    if ts_props:
        ts_binding_id = guid("tsbinding", entity, spec["ts_table"])
        parts.append(part(f"EntityTypes/{eid}/DataBindings/{ts_binding_id}.json", {
            "id": ts_binding_id,
            "dataBindingConfiguration": {
                "dataBindingType": "TimeSeries",
                "timestampColumnName": spec["ts_column"],
                "propertyBindings": [
                    # each observation row attaches to its entity instance via the key
                    {"sourceColumnName": spec["properties"][k][0],
                     "targetPropertyId": big_id("prop", entity, k)}
                    for k in spec["keys"]
                ] + [
                    {"sourceColumnName": column, "targetPropertyId": big_id("prop", entity, name)}
                    for name, (column, _) in ts_props.items()
                ],
                "sourceTableProperties": {**source_table, "sourceTableName": spec["ts_table"]},
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
        "description": "NEM ontology v3: fct_summary bound directly as time series on Unit",
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
print(f"Refresh triggered on {graph['displayName']} (takes a while: 10.5M time-series rows)")
