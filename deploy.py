import os
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
print("=== Deploy complete ===")