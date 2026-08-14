import os
import runpy

import duckrun

WORKSPACE      = "91588e42-0f1c-4e56-bcaa-cbf015b8f312"  # analytics_as_code
LAKEHOUSE      = "data"
FOLDER         = "aemo"   # workspace folder every deployed item lands in
SCHEDULE_EVERY = "720m"
DOWNLOAD_LIMIT = "5"
PROCESS_LIMIT  = "100"

os.chdir(os.path.dirname(os.path.abspath(__file__)))
ws = duckrun.workspace(WORKSPACE)
ws.create_lakehouse(LAKEHOUSE, folder=FOLDER)   # idempotent
files = duckrun.connect(f"{ws.display_name}/{LAKEHOUSE}.Lakehouse")
files.copy("dbt", "dbt", overwrite=True)
ws.deploy("fabric_items", lakehouse=LAKEHOUSE, folder=FOLDER, overwrite=True, variables={
    "download_limit": DOWNLOAD_LIMIT,
    "process_limit":  PROCESS_LIMIT,
    "lakehouse_name": LAKEHOUSE,
    "workspace_id":   ws.id,
})
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