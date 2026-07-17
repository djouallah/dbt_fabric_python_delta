import argparse
import json
import os
import tempfile
from pathlib import Path

import duckrun
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--env", default="prod")
parser.add_argument(
    "--full", action="store_true",
    help="Deploy the Fabric items: the semantic model, notebook, variable library and data "
         "pipeline (+ schedule), plus the dbt project files the notebook reads from OneLake. "
         "Without this flag the scope is NONE — only the always-required lakehouse is "
         "provisioned and nothing else is deployed. The real orchestrator is GitHub Actions; "
         "the Fabric notebook/pipeline are only a fun demo of in-Fabric scheduling.",
)
args = parser.parse_args()

root       = Path(__file__).parent
all_cfg    = yaml.safe_load((root / "deploy_config.yml").read_text())
if args.env not in all_cfg:
    raise SystemExit(f"No '{args.env}' section in deploy_config.yml. Add it for this branch.")
cfg        = {**all_cfg.get("defaults", {}), **all_cfg[args.env]}
WS_ID      = cfg["ws"]
LH_NAME    = cfg["lakehouse_name"]
dbt        = root / "dbt"
fabric_items = root / "fabric_items"

# Lakehouse is always provisioned (it holds the data); the scope is binary — NONE (lakehouse
# only) or FULL (every Fabric item). Default is NONE.
print(f"Deploy scope: {'FULL (semantic model + notebook/VL/pipeline)' if args.full else 'NONE (lakehouse only)'}")


def find_item(item_type):
    """Derive an item's display name from its fabric_items/<name>.<Type> folder."""
    matches = list(fabric_items.glob(f"*.{item_type}"))
    if len(matches) != 1:
        raise SystemExit(f"Expected exactly one {item_type} in fabric_items/, found {len(matches)}")
    return matches[0].name.removesuffix(f".{item_type}")


# duckrun uses FABRIC_TOKEN / POWERBI_TOKEN from the env in CI, or an `az login` session locally.
ws = duckrun.workspace(WS_ID)

# 1. Ensure the lakehouse exists (idempotent; schema-enabled), returns its item id.
print("=== 1. Create lakehouse ===")
lh_id = ws.create_lakehouse(LH_NAME)
print(f"Lakehouse '{LH_NAME}' id: {lh_id}")

# Scope NONE stops here: the lakehouse is the only always-required item. Everything below
# (semantic model + notebook/VL/pipeline) is the FULL scope, opted into with --full.
if not args.full:
    print("Scope NONE — lakehouse ensured, skipping all Fabric item deploys (pass --full to deploy them).")
    raise SystemExit(0)

NB_NAME = find_item("Notebook")
PL_NAME = find_item("DataPipeline")
SM_NAME = find_item("SemanticModel")
VL_NAME = find_item("VariableLibrary")

# Item display names must be passed explicitly — deploy() otherwise defaults to the source
# filename stem (notebook-content / variables / pipeline-content / model), not the item name.

# 2. Deploy notebook (capture its new id for the pipeline notebook-reference patch below).
print("=== 2. Deploy notebook ===")
nb_id = ws.deploy(str(fabric_items / f"{NB_NAME}.Notebook" / "notebook-content.ipynb"),
                  name=NB_NAME, overwrite=True)
print(f"Notebook '{NB_NAME}' id: {nb_id}")

# 3. Deploy variable library — inject the env-specific values at deploy time (no file edit,
#    no git-checkout restore).
print("=== 3. Deploy variable library ===")
ws.deploy(str(fabric_items / f"{VL_NAME}.VariableLibrary" / "variables.json"),
          name=VL_NAME, overwrite=True, variables={
              "download_limit": cfg["download_limit"],
              "process_limit":  cfg["process_limit"],
              "lakehouse_name": LH_NAME,
              "workspace_id":   WS_ID,
          })

# 4. Copy the dbt project to OneLake Files/dbt (the notebook reads the project from there).
#    duckrun streams every file in-process over obstore and raises on failure.
print("=== 4. Copy dbt files to OneLake ===")
files = duckrun.connect(
    f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables",
    storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})
files.copy(str(dbt), "dbt", overwrite=True)

# 5. Deploy semantic model — duckrun repoints the OneLake workspace/lakehouse GUIDs baked into
#    the Direct Lake model to LH_NAME and refreshes it (reframe) before returning.
print("=== 5. Deploy semantic model ===")
ws.deploy(str(fabric_items / f"{SM_NAME}.SemanticModel" / "model.bim"),
          name=SM_NAME, lakehouse=LH_NAME, overwrite=True)

# 6. Deploy data pipeline — point both TridentNotebook activities (low-core run + high-core
#    retry) at the freshly deployed notebook and the target workspace. duckrun deploys the
#    pipeline verbatim, so patch the notebook reference in-memory first.
print("=== 6. Deploy pipeline ===")
pl_path = fabric_items / f"{PL_NAME}.DataPipeline" / "pipeline-content.json"
pl = json.loads(pl_path.read_text())
for act in pl["properties"]["activities"]:
    tp = act.get("typeProperties", {})
    if "notebookId" in tp:
        tp["notebookId"] = nb_id
    if "workspaceId" in tp:
        tp["workspaceId"] = WS_ID
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
    json.dump(pl, f)
    tmp_pl = f.name
try:
    ws.deploy(tmp_pl, name=PL_NAME, overwrite=True)
finally:
    os.unlink(tmp_pl)

# 7. Schedule the pipeline (idempotent — re-scheduling updates the existing schedule rather
#    than stacking a duplicate).
print("=== 7. Schedule pipeline ===")
ws.schedule(PL_NAME, every=f"{cfg['schedule_interval']}m")

print("=== Deploy complete ===")
