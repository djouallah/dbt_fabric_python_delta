# Fire a RefreshGraph job on the aemo_nem graph model, so the graph the Operations Agent
# monitors reflects the data a run just landed. Only SCHEMA changes auto-refresh; data
# changes need this explicit job, and the jobType is undocumented -- `Refresh` and
# `GraphRefresh` both return InvalidJobType (see CLAUDE.md).
#
# Deliberately tolerant: this runs after every CI data load, including workspaces where the
# ontology was never deployed and moments when a refresh is already in flight (Fabric
# rejects the second POST). None of that should fail a data load, so every outcome prints
# and exits 0.

import os

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = os.environ.get("WS_ID", "450bf196-431f-463f-9316-2d1ce1da98db")  # sqlengines
ONTOLOGY  = "aemo_nem"
API       = "https://api.fabric.microsoft.com/v1"

try:
    headers = {"Authorization": f"Bearer {get_fabric_token()}"}
    models = requests.get(f"{API}/workspaces/{WORKSPACE}/GraphModels", headers=headers)
    models.raise_for_status()
    graph = next((m for m in models.json().get("value", [])
                  if m["displayName"].startswith(f"{ONTOLOGY}_graph")), None)
    if graph is None:
        print(f"No {ONTOLOGY}_graph* model in the workspace -- nothing to refresh.")
        raise SystemExit(0)
    resp = requests.post(f"{API}/workspaces/{WORKSPACE}/items/{graph['id']}/jobs/instances"
                         "?jobType=RefreshGraph", headers=headers)
    if resp.ok:
        print(f"RefreshGraph triggered on {graph['displayName']}")
    else:
        # Most likely a refresh already in flight; the next load catches up either way.
        print(f"RefreshGraph not started ({resp.status_code}): {resp.text[:200]}")
except SystemExit:
    raise
except Exception as e:
    print(f"Graph refresh skipped: {e}")
