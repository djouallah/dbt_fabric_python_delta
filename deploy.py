"""Deploy the Fabric items for this project.

Everything runs through the duckrun workspace API: one folder deploy ships the whole
`fabric_items/` tree (Fabric git-integration layout) in dependency order — variable
library, notebook, semantic model, then data pipeline — rewriting the Direct Lake GUIDs
and wiring the pipeline's notebook activities automatically.

GitHub Actions is the real orchestrator; the notebook + pipeline are the in-Fabric
scheduling demo.
"""
import os

import duckrun

WORKSPACE      = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
LAKEHOUSE      = "data"
FOLDER         = "aemo"   # workspace folder every deployed item lands in
SCHEDULE_EVERY = "720m"
# Injected into the deploy_config VariableLibrary — drives the in-Fabric notebook run.
DOWNLOAD_LIMIT = "5"
PROCESS_LIMIT  = "100"

os.chdir(os.path.dirname(os.path.abspath(__file__)))
ws = duckrun.workspace(WORKSPACE)
lakehouse_id = ws.create_lakehouse(LAKEHOUSE, folder=FOLDER)

# dbt project -> Files/dbt (the Fabric notebook runs it from there).
files = duckrun.connect(
    f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables")
files.copy("dbt", "dbt", overwrite=True)

# One call: VariableLibrary -> Notebook -> SemanticModel -> DataPipeline, in order.
# lakehouse= repoints the Direct Lake model (and refreshes it); the pipeline's notebook
# activities are auto-wired to the folder's single notebook.
ws.deploy("fabric_items", lakehouse=LAKEHOUSE, folder=FOLDER, overwrite=True, variables={
    "download_limit": DOWNLOAD_LIMIT,
    "process_limit":  PROCESS_LIMIT,
    "lakehouse_name": LAKEHOUSE,
    "workspace_id":   ws.id,
})

ws.schedule("run_pipeline", every=SCHEDULE_EVERY)

print("=== Deploy complete ===")
