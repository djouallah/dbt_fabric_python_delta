# Read a deployed Fabric item's definition back out of the workspace, onto disk.
#
# Everything else in this repo pushes one way: ontology.py builds its parts from the ENTITIES /
# RELATIONSHIPS dicts, data_agent.py builds its parts from the INSTRUCTIONS string, and both POST
# straight to Fabric. Nothing reads back, so there is no way to see what the service ACTUALLY
# stored -- which matters for the two bets those scripts make (the undocumented "ontology"
# dataSourceType, and the __LATEST_DATE__ constant baked into aiInstructions at deploy time).
#
# getDefinition is documented for both item types and returns the git-integration part layout:
#   ontologies   -> .platform, definition.json, EntityTypes/{id}/..., RelationshipTypes/{id}/...
#   dataAgents   -> Files/Config/data_agent.json, Files/Config/draft/..., and once published
#                   Files/Config/publish_info.json + the whole Files/Config/published/ mirror
# The Data Agent parts table has NO .platform -- the same fact that forces data_agent.py to omit
# ?updateMetadata=true. Its absence in the downloaded folder is correct, not a truncated download.
#
# This is an INSPECTION tool, not a round trip. ontology.py and data_agent.py generate their parts
# in Python and never read the filesystem; they remain the source of truth. The output folder is
# gitignored, and deliberately NOT fabric_items/ -- duckrun's folder deploy knows only
# VariableLibrary / Notebook / SemanticModel / DataPipeline and would raise "unsupported item type"
# on an .Ontology folder, breaking the whole deploy.
#
# Usage:
#   python download.py                             # the DEFAULT_TARGETS below
#   python download.py ontologies                  # every ontology in the workspace
#   python download.py dataAgents aemo_nem_agent   # one item by displayName
#   python download.py GraphModels                 # best-effort; getDefinition is undocumented here

import base64
import os
import sys
import time

import requests
from duckrun.auth import get_fabric_token

# Same env vocabulary + default as every other script -- nothing to keep in sync by hand.
WORKSPACE = os.environ.get("WS_ID", "450bf196-431f-463f-9316-2d1ce1da98db")  # sqlengines
API       = "https://api.fabric.microsoft.com/v1"
OUT       = "fabric_download"   # gitignored

DEFAULT_TARGETS = [("ontologies", "aemo_nem"), ("dataAgents", "aemo_nem_agent")]

# Folder suffix per collection, used only when the list response carries no "type" of its own.
ITEM_TYPE = {"ontologies": "Ontology", "dataAgents": "DataAgent", "GraphModels": "GraphModel"}

os.chdir(os.path.dirname(os.path.abspath(__file__)))

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


def get_definition(collection, item_id):
    """The item's definition parts. Not call(): getDefinition may answer synchronously (200) OR
    as an LRO whose payload lives at {Location}/result -- call() would hand back the
    operation-STATUS body, which has no definition in it."""
    resp = session.post(f"{API}/workspaces/{WORKSPACE}/{collection}/{item_id}/getDefinition")
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
    if not resp.ok:
        raise RuntimeError(f"getDefinition {collection}/{item_id} -> "
                           f"{resp.status_code}: {resp.text[:400]}")
    return resp.json()["definition"]["parts"]


def resolve(collection, name=None):
    """[(displayName, id, itemType)] for a collection, optionally filtered to one displayName.

    Ontology, GraphModel and DataAgent do NOT appear in GET /workspaces/{ws}/items at all, so
    the per-type collection is the only listing that finds them."""
    items = call("GET", f"{API}/workspaces/{WORKSPACE}/{collection}").json()["value"]
    if name is not None:
        items = [i for i in items if i["displayName"] == name]
        if not items:
            found = [i["displayName"] for i in
                     call("GET", f"{API}/workspaces/{WORKSPACE}/{collection}").json()["value"]]
            raise SystemExit(f"No item named '{name}' in {collection}. Found: {found or 'none'}")
    return [(i["displayName"], i["id"], i.get("type") or ITEM_TYPE.get(collection, collection))
            for i in items]


def write(display_name, item_type, parts):
    """Decode the parts into OUT/{displayName}.{ItemType}/ and return that path."""
    target = os.path.join(OUT, f"{display_name}.{item_type}")
    for p in parts:
        rel = p["path"]
        # Part paths come from the service; never let one escape the item folder.
        if os.path.isabs(rel) or ".." in rel.replace("\\", "/").split("/"):
            raise RuntimeError(f"refusing to write definition part {rel!r} outside {target!r}")
        dest = os.path.join(target, *rel.replace("\\", "/").split("/"))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        # Bytes verbatim, no re-indenting -- the point is to see exactly what Fabric stored.
        with open(dest, "wb") as f:
            f.write(base64.b64decode(p["payload"]))
    return target


args = sys.argv[1:]
if args:
    targets = [(args[0], args[1] if len(args) > 1 else None)]
else:
    targets = DEFAULT_TARGETS

for collection, name in targets:
    for display_name, item_id, item_type in resolve(collection, name):
        parts = get_definition(collection, item_id)
        target = write(display_name, item_type, parts)
        print(f"\n{display_name} ({item_type}, {item_id}) -> {target}  [{len(parts)} parts]")
        for p in sorted(parts, key=lambda p: p["path"]):
            print(f"  {p['path']}")
