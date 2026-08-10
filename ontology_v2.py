# aemo_nem_v2: the same NEM ontology rebuilt on two doc-verified capabilities that v1
# (ontology.py) worked around, deployed as a SEPARATE item so v1 stays the control:
#
# 1. COMPOSITE ENTITY KEYS. entityIdParts is documented as "the properties (by ID) that
#    together uniquely identify an entity", and contextualization key-ref bindings are
#    arrays ("the columns ... making up the unique ID"). So UnitDay is keyed
#    [DUID, DateKey] directly -- no concatenated UnitDayKey surrogate -- and the
#    PRODUCED edge binds two target key columns. Keys still cannot be dates (String or
#    Integer only), so the date leg stays the ISO DateKey string.
#
# 2. NO GLOBAL NAME PREFIXING. Docs: "Property names can only be duplicated across
#    entities for properties of the same type." So Station.Region and Unit.Region are
#    both plain "Region" (String), RegCapMW repeats as Double, and UnitDay carries a
#    plain DUID. v1 prefixed everything on the belief names were globally unique.
#
# Both claims come from the docs, and this product has already shipped one
# documented-shape-that-silently-fails (DECIMAL(p,s) -> NULL), so the deploy is only
# half the test: verification must count nodes and traverse edges in the graph.

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
ONTOLOGY  = "aemo_nem_v2"
API       = "https://api.fabric.microsoft.com/v1"

# propertyName: (sourceColumn, valueType). "keys" is a LIST -- the composite-key test.
# "display" picks the display-name property when it isn't the first key.
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
    # The reified PRODUCED event, natural-keyed: [DUID, DateKey] as entityIdParts.
    "UnitDay": {
        "table": "agg_unit_daily",
        "keys": ["DUID", "DateKey"],
        "display": "DateKey",
        "properties": {
            "DUID":          ("DUID",          "String"),
            "DateKey":       ("DateKey",       "String"),
            "Date":          ("date",          "DateTime"),
            "GenerationMWh": ("GenerationMWh", "Double"),
            "PeakMW":        ("PeakMW",        "Double"),
            "AvgMW":         ("AvgMW",         "Double"),
            "IntervalCount": ("IntervalCount", "BigInt"),
        },
    },
}

# (name, source entity, target entity, mapping table, source bindings, target bindings).
# Bindings are LISTS of (mapping column, key property) pairs -- PRODUCED's target uses
# two, matching UnitDay's composite key.
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
    ("PRODUCED",      "Unit",           "UnitDay",     "agg_unit_daily",
     [("DUID", "DUID")],                         [("DUID", "DUID"), ("DateKey", "DateKey")]),
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
        "description": "NEM ontology v2: composite entity keys, unprefixed property names",
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
print(f"Refresh triggered on {graph['displayName']} (takes a couple of minutes)")
