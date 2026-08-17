import json
import os
import re
import runpy

import duckrun

WORKSPACE      = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
# Two lakehouses. LAKEHOUSE holds every Delta table dbt builds plus the dbt project itself
# under Files/dbt; LAKEHOUSE_LANDING holds the raw AEMO archive under Files/ and is never
# written by dbt's Tables output. The landing one is NOT created here — it predates this
# script and outlives every run.
LAKEHOUSE         = "data"
LAKEHOUSE_LANDING = "data_landing"
FOLDER         = "aemo"   # workspace folder every deployed item lands in
SCHEDULE_EVERY = "720m"
DOWNLOAD_LIMIT = "5"

# DEPLOY_SEMANTIC_MODEL=false deploys everything EXCEPT the Direct Lake model. Nothing
# downstream needs it -- the pipeline has only notebook activities, and the ontology and data
# agent bind to the mart Delta tables -- so it is genuinely optional, and skipping it also
# skips duckrun's post-deploy reframe, which is the slow part of a deploy. Read from the
# environment rather than sys.argv: data_agent.py scans argv for --dump and runs under runpy
# with THIS script's argv, so a CLI flag here would leak into it.
DEPLOY_MODEL = os.environ.get("DEPLOY_SEMANTIC_MODEL", "true").lower() != "false"

os.chdir(os.path.dirname(os.path.abspath(__file__)))
ws = duckrun.workspace(WORKSPACE)
ws.create_lakehouse(LAKEHOUSE, folder=FOLDER)   # idempotent
if LAKEHOUSE_LANDING not in [l["displayName"] for l in ws.list_lakehouses()]:
    raise SystemExit(f"landing lakehouse '{LAKEHOUSE_LANDING}' not found — it holds the raw "
                     f"archive and is not created here")
# The dbt project lives in the TRANSFORM lakehouse: it is the one the notebook mounts, and
# keeping it out of the landing lakehouse leaves that one holding nothing but raw AEMO files.
files = duckrun.connect(f"{ws.display_name}/{LAKEHOUSE}.Lakehouse")
files.copy("dbt", "dbt", overwrite=True)
ws.deploy("fabric_items", folder=FOLDER, overwrite=True, variables={
    "download_limit":         DOWNLOAD_LIMIT,
    "lakehouse_name":         LAKEHOUSE,
    "lakehouse_landing_name": LAKEHOUSE_LANDING,
    "workspace_id":           ws.id,
})

def check_bim(conn):
    """Fail here, not in Fabric, when the bim references something the tables don't have.

    A Direct Lake model that binds a missing column does not fail at deploy with a useful
    message: Fabric either rejects the import with `Workload_FailedToParseFile` naming an
    object id, or accepts it and fails the REFRESH with a Delta protocol violation, minutes
    later. Both cost a full pipeline run to discover. This checks the three ways the bim can
    go stale after a mart schema change -- a column that no longer exists, a relationship
    endpoint that was never added, and DAX naming a dropped column -- against the tables that
    were just built. Every one of these has actually happened here.
    """
    with open("semantic_model/aemo_electricity.SemanticModel/model.bim", encoding="utf-8-sig") as f:
        model = json.load(f)["model"]
    cols = {t["name"]: {c["name"] for c in t.get("columns", [])} for t in model["tables"]}
    problems = []
    for t in model["tables"]:
        src = t["partitions"][0]["source"]
        schema, entity = src.get("schemaName", "mart"), src.get("entityName", t["name"])
        want = {c["sourceColumn"] for c in t.get("columns", []) if "sourceColumn" in c}
        have = {r[0] for r in conn.sql(f'DESCRIBE "{schema}"."{entity}"').fetchall()}
        problems += [f"{t['name']}.{c}: not in {schema}.{entity}" for c in sorted(want - have)]
    for r in model.get("relationships", []):
        for side in ("from", "to"):
            table, column = r[f"{side}Table"], r[f"{side}Column"]
            if column not in cols.get(table, set()):
                problems.append(f"relationship {r.get('name', '?')}: {table}[{column}] undefined")
    measures = {m["name"] for t in model["tables"] for m in t.get("measures", [])}
    for t in model["tables"]:
        for m in t.get("measures", []):
            for table, column in re.findall(r"(\w+)\[([^\]]+)\]", m["expression"]):
                known = {c.lower() for c in cols.get(table, set())} | {x.lower() for x in measures}
                if column.lower() not in known:
                    problems.append(f"measure {m['name']}: {table}[{column}] undefined")
    if problems:
        raise SystemExit("semantic model is stale against the mart:\n  " + "\n  ".join(problems))
    print(f"semantic model OK ({len(model['tables'])} tables, "
          f"{len(model.get('relationships', []))} relationships)")


# The semantic model deploys from its own folder because duckrun's deploy() takes no exclude
# filter -- a single fabric_items/ call is all-or-nothing. Splitting it out is what makes it
# skippable. fabric_items/ still holds exactly one notebook, so the pipeline's notebook
# activities are still auto-wired; lakehouse= lives here now because the Direct Lake bim is
# the only thing that ever consumed it.
if DEPLOY_MODEL:
    check_bim(files)
    ws.deploy("semantic_model", lakehouse=LAKEHOUSE, folder=FOLDER, overwrite=True)
else:
    print("=== Skipping the semantic model (DEPLOY_SEMANTIC_MODEL=false) ===")

ws.schedule("run_pipeline", every=SCHEDULE_EVERY)

# The ontology and the data agent are Fabric items like any other -- they just live outside
# fabric_items/ because duckrun's folder deploy only knows VariableLibrary, Notebook,
# SemanticModel and DataPipeline, and raises "unsupported item type" on anything else. So
# they are pushed by their own scripts here, in the same deploy step as everything else,
# rather than from a laptop. Order matters: the agent binds the ontology by displayName.
#
# run_path rather than import: both are flat scripts that do their work at module level,
# the same shape as this file.
runpy.run_path("ontology.py", run_name="__main__")
runpy.run_path("data_agent.py", run_name="__main__")

print("=== Deploy complete ===")