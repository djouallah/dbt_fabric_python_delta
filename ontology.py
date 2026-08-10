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
ONTOLOGY  = "aemo_nem"
API       = "https://api.fabric.microsoft.com/v1"

# propertyName: (sourceColumn, valueType). Property names are unique across the whole
# ontology, not per entity type, so anything that appears on two entities is prefixed.
ENTITIES = {
    "Region": {
        "table": "dim_region",
        "key": "RegionID",
        "properties": {
            "RegionID":     ("RegionID", "String"),
            "RegionState":  ("State",    "String"),
            "RegionMarket": ("Market",   "String"),
        },
    },
    "Interconnector": {
        "table": "dim_interconnector",
        "key": "InterconnectorID",
        # No row filter is possible at bind time, so EnergyConnect (InService = false)
        # becomes a node like any other. Filter on InterconnectorInService in any
        # reachability query or SA1 gains a direct NSW1 link that does not exist.
        "properties": {
            "InterconnectorID":        ("InterconnectorID", "String"),
            "InterconnectorName":      ("Name",             "String"),
            "InterconnectorAcDc":      ("AcDc",             "String"),
            "InterconnectorInService": ("InService",        "Boolean"),
        },
    },
    "Participant": {
        "table": "dim_participant",
        "key": "Participant",
        "properties": {
            "Participant":             ("Participant",             "String"),
            "ParentParticipant":       ("ParentParticipant",       "String"),
            "IsCurated":               ("IsCurated",               "Boolean"),
            "IsRegisteredParticipant": ("IsRegisteredParticipant", "Boolean"),
        },
    },
    "Station": {
        "table": "dim_station",
        "key": "StationName",
        "properties": {
            "StationName":               ("StationName",             "String"),
            "StationRegion":             ("Region",                  "String"),
            "StationUnitCount":          ("UnitCount",               "BigInt"),
            "StationGeneratingUnits":    ("GeneratingUnitCount",     "BigInt"),
            "StationLoadUnits":          ("LoadUnitCount",           "BigInt"),
            "StationBidirectionalUnits": ("BidirectionalUnitCount",  "BigInt"),
            "StationRegCapMW":           ("RegCapMW",                "Double"),
        },
    },
    # Generation at region x date. fct_summary itself (~140M rows, DUID x 5 minutes) is far
    # too large and too fine-grained for a graph that fully re-ingests on every save, and its
    # split date/time columns cannot form an ontology timestamp anyway.
    "RegionDay": {
        "table": "agg_region_daily",
        "key": "RegionDayKey",
        "properties": {
            "RegionDayKey":           ("RegionDayKey",  "String"),
            "RegionDayRegionID":      ("Region",        "String"),
            "RegionDayDate":          ("date",          "DateTime"),
            "RegionDayGenerationMWh": ("GenerationMWh", "Double"),
            "RegionDayPeakMW":        ("PeakMW",        "Double"),
            "RegionDayAvgMW":         ("AvgMW",         "Double"),
            "RegionDayAvgPrice":      ("AvgPrice",      "Double"),
            "RegionDayIntervalCount": ("IntervalCount", "BigInt"),
        },
    },
    # Generation at unit x date -- the reified PRODUCED event. Daily is the finest grain
    # the graph can carry (~100k rows vs fct_summary's 140M); 5-minute detail stays SQL-only.
    "UnitDay": {
        "table": "agg_unit_daily",
        "key": "UnitDayKey",
        "properties": {
            "UnitDayKey":           ("UnitDayKey",    "String"),
            "UnitDayDUID":          ("DUID",          "String"),
            "UnitDayDate":          ("date",          "DateTime"),
            # ISO date as a String: GQL has no date literals, but ISO strings order
            # lexicographically, so range filters work on this property.
            "UnitDayDateKey":       ("DateKey",       "String"),
            "UnitDayGenerationMWh": ("GenerationMWh", "Double"),
            "UnitDayPeakMW":        ("PeakMW",        "Double"),
            "UnitDayAvgMW":         ("AvgMW",         "Double"),
            "UnitDayIntervalCount": ("IntervalCount", "BigInt"),
        },
    },
    "Unit": {
        "table": "dim_duid",
        "key": "DUID",
        "properties": {
            "DUID":             ("DUID",                 "String"),
            "UnitRegion":       ("Region",               "String"),
            "UnitFuelSource":   ("FuelSourceDescriptor", "String"),
            "UnitDispatchType": ("DispatchType",         "String"),
            "UnitTechnology":   ("TechnologyType",       "String"),
            "UnitRegCapMW":     ("RegCapMW",             "Double"),
            "UnitStationName":  ("StationName",          "String"),
        },
    },
}

# (name, source entity, target entity, mapping table, source column, target column).
# The mapping table must carry both keys on the same row. SUBSIDIARY_OF is the
# recursive one: same entity type at both ends, two different columns.
RELATIONSHIPS = [
    ("SUBSIDIARY_OF", "Participant",    "Participant", "dim_participant_parent", "Participant",      "ParentParticipant"),
    ("OPERATED_BY",   "Unit",           "Participant", "dim_duid",               "DUID",             "Participant"),
    ("PART_OF",       "Unit",           "Station",     "dim_duid",               "DUID",             "StationName"),
    ("OWNED_BY",      "Station",        "Participant", "dim_station",            "StationName",      "Participant"),
    ("LOCATED_IN",    "Station",        "Region",      "dim_station",            "StationName",      "Region"),
    ("CONNECTS_FROM", "Interconnector", "Region",      "dim_interconnector",     "InterconnectorID", "FromRegion"),
    ("CONNECTS_TO",   "Interconnector", "Region",      "dim_interconnector",     "InterconnectorID", "ToRegion"),
    ("GENERATION_IN", "RegionDay",      "Region",      "agg_region_daily",       "RegionDayKey",     "Region"),
    ("PRODUCED",      "Unit",           "UnitDay",     "agg_unit_daily",         "DUID",             "UnitDayKey"),
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
    key_pid = big_id("prop", entity, spec["key"])
    parts.append(part(f"EntityTypes/{eid}/definition.json", {
        "id": eid,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": entity,
        "entityIdParts": [key_pid],
        "displayNamePropertyId": key_pid,
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

for name, src, tgt, table, src_col, tgt_col in RELATIONSHIPS:
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
            {"sourceColumnName": src_col, "targetPropertyId": big_id("prop", src, ENTITIES[src]["key"])}
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": tgt_col, "targetPropertyId": big_id("prop", tgt, ENTITIES[tgt]["key"])}
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
        "description": "NEM ontology generated from the dbt dimension tables",
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
