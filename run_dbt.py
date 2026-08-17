# Run one dbt command, on Fabric compute or on this machine.
#
#   python run_dbt.py run          # the models
#   python run_dbt.py test         # the tests, minus tag:heavy
#   python run_dbt.py docs         # docs generate (always LOCAL -- see below)
#
# DBT_RUNNER picks where the work happens:
#
#   remote (default)  duckrun's RemoteRunner -- zips the project, ships it to a throwaway
#                     Fabric Python notebook of DBT_CORES vCores, streams the dbt log back,
#                     deletes the notebook. The compute is data-local to OneLake, so the
#                     reads never leave the region; a GitHub runner pulls every byte across
#                     the internet twice (read, then write).
#   local             dbt.cli.main.dbtRunner in this process, exactly as before.
#
# RemoteRunner is a drop-in for dbtRunner, so the only difference is which object gets
# constructed. Two things do NOT carry over, and both are handled here:
#
#   1. RemoteRunner REQUIRES a full abfss:// root_path -- it rejects duckrun's OneLake
#      shorthand (`<workspace>/<item>.Lakehouse`). ONELAKE_TABLES_PATH is normalised below.
#   2. The remote run's dbt artifacts stay on the remote notebook: no local
#      target/run_results.json comes back, only {node, status} per node. The CI dashboard
#      reads run_results.json, so a compatible file is synthesised. It carries NO
#      execution_time -- the remote result does not report per-node timing -- so the
#      dashboard's "slowest models" table reads 0s under the remote runner. That is a known
#      gap, not a build that took no time.
#
# `docs` always runs locally: it needs the local target/ to publish to Pages, and it is a
# catalog query rather than a build, so there is nothing to gain from remote compute.

import json
import os
import re
import shutil
import sys
import time

CORES = int(os.environ.get("DBT_CORES", "8"))
RUNNER = os.environ.get("DBT_RUNNER", "remote").strip().lower()
ARGS = ["--target", "dev", "--project-dir", "dbt", "--profiles-dir", "dbt"]

COMMANDS = {
    "run":  (["run",  *ARGS],                              "/tmp/rr_0.json",        "model"),
    "test": (["test", *ARGS, "--exclude", "tag:heavy"],    "/tmp/test_results.json", "test"),
    "docs": (["docs", "generate", *ARGS],                  None,                     None),
}

if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
    raise SystemExit(f"usage: python run_dbt.py {{{'|'.join(COMMANDS)}}}")
command = sys.argv[1]
argv, snapshot_to, node_kind = COMMANDS[command]

# duckrun expands `<workspace>/<item>.Lakehouse` itself, but RemoteRunner does not -- it needs
# the abfss URL to locate the workspace and lakehouse it must run the notebook against.
root = os.environ.get("ONELAKE_TABLES_PATH", "")
if RUNNER == "remote" and not root.startswith("abfss://"):
    raise SystemExit(f"remote runner needs an abfss:// ONELAKE_TABLES_PATH; got {root!r}")

if RUNNER == "remote" and command != "docs":
    from duckrun import RemoteRunner
    dbt = RemoteRunner(cores=CORES)
    where = f"Fabric ({CORES} vCores)"
else:
    from dbt.cli.main import dbtRunner
    dbt = dbtRunner()
    where = "local"

print(f"=== dbt {command} on {where} ===", flush=True)
t0 = time.time()
res = dbt.invoke(argv)
elapsed = int(time.time() - t0)


def snapshot(dest):
    """Leave a run_results.json where the CI dashboard expects it."""
    local_artifact = os.path.join("dbt", "target", "run_results.json")
    if os.path.exists(local_artifact):        # local runner: dbt wrote the real thing
        shutil.copy(local_artifact, dest)
        return
    rows = []
    for r in (res.result or []):
        # remote reports {"node": name}; local reports a RunResult whose .node is an object
        name = (r.get("node", "?") if isinstance(r, dict)
                else getattr(getattr(r, "node", None), "name", "?"))
        status = r.get("status", "?") if isinstance(r, dict) else getattr(r, "status", "?")
        # The dashboard filters run results on a `model.` prefix and counts test rows, so the
        # id has to be prefixed even though the remote result reports a bare node name.
        rows.append({"unique_id": f"{node_kind}.{name}", "status": str(status),
                     "execution_time": 0})
    with open(dest, "w", encoding="utf-8") as f:
        json.dump({"results": rows}, f)


if snapshot_to:
    snapshot(snapshot_to)

# Phase timings for the dashboard, when running under Actions.
github_env = os.environ.get("GITHUB_ENV")
if github_env:
    key = {"run": "T_DBT_RUN", "test": "T_DBT_TEST", "docs": "T_DBT_DOCS"}[command]
    with open(github_env, "a", encoding="utf-8") as f:
        f.write(f"{key}={elapsed}\n")
        if command == "run":
            f.write(f"DBT_RUNNER_USED={where}\n")

failed = [r for r in (res.result or [])
          if str(r.get("status") if isinstance(r, dict) else getattr(r, "status", "")).lower()
          not in ("success", "pass", "skipped")]
for r in failed:
    name = r.get("node", "?") if isinstance(r, dict) else getattr(r, "node", "?")
    status = r.get("status", "?") if isinstance(r, dict) else getattr(r, "status", "?")
    print(f"  {status:10} {name}")
print(f"=== dbt {command}: success={res.success} in {elapsed}s "
      f"({len(res.result or [])} nodes, {len(failed)} not ok) ===", flush=True)

sys.exit(0 if res.success else 1)
