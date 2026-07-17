# Fabric Deploy — Lessons Learned

`deploy.py` deploys the Fabric items through the **duckrun workspace API**
(`duckrun.workspace(...)`), not the `fab` CLI. GitHub Actions is the real orchestrator;
the notebook/pipeline are an in-Fabric scheduling demo. Scope is binary: NONE (lakehouse
only, default) vs FULL (`--full`, every item).

## Tokens (no `fab auth login`, no `az login`, no token step)

duckrun mints every token it needs itself — OneLake (storage), Fabric control plane, and
Power BI (semantic-model refresh) — directly from the GitHub Actions **OIDC JWT** via
workload-identity federation (`_github_oidc_token`, first in each acquisition chain). The
workflow just needs `id-token: write` and `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` in the env;
there is **no** `azure/login` step and **no** `az account get-access-token` minting. Tokens
are acquired lazily per audience, so a `deploy=none` run never touches the Power BI scope.

Locally, an `az login` session covers all three (duckrun falls back to azure-identity's
`AzureCliCredential`). dbt (`dbt/profiles.yml`) sets **no** `storage_options` token — the
duckrun adapter auto-acquires the OneLake token the same way.

## What duckrun does for you

- **`ws.create_lakehouse(name)`** — idempotent, schema-enabled, returns the item id.
  No `fab exists`/`create`, no post-create sleep.
- **Item names must be passed explicitly** — `ws.deploy(path, name=...)`. Without `name=`,
  deploy defaults to the source **filename stem** (`notebook-content` / `variables` /
  `pipeline-content` / `model`), which is not the item's display name. `deploy.py` derives
  names from the `fabric_items/<name>.<Type>` folders via `find_item()`.
- **Semantic model** — `ws.deploy("model.bim", lakehouse=LH_NAME, overwrite=True)`
  auto-repoints the OneLake workspace/lakehouse GUIDs baked into the Direct Lake model to
  the target lakehouse **and refreshes** (reframe) before returning. No regex substitution,
  no bim cache, no separate refresh step. The dev GUIDs stay in the checked-in `model.bim`.
- **Pipeline** — deployed **verbatim** (no GUID rewrite), so patch the notebook reference
  in-memory first: set each `properties.activities[*].typeProperties.notebookId` to the id
  returned by the notebook deploy and `.workspaceId` to the target workspace, then deploy
  the patched JSON. Deploy the notebook first to get its id.
- **Variable library** — `ws.deploy("variables.json", variables={...}, overwrite=True)`
  injects env-specific values at deploy time; no file edit + git-checkout restore.
- **`ws.schedule(name, every="30m")`** — idempotent, updates the existing schedule instead
  of stacking duplicates. No list/dedup logic. duckrun sets start=now, end=2099 (so
  `schedule_start`/`schedule_end` config keys are gone); only `schedule_interval` is read.
- **`duckrun.connect(tables_path, storage_options={"bearer_token": ONELAKE_TOKEN}).copy(dbt, "dbt", overwrite=True)`**
  streams the dbt project to `Files/dbt` in-process over obstore and raises on failure
  (the notebook reads the project from there).

## Deploy order for Direct Lake semantic models

Direct Lake validation requires the Delta tables to exist before the semantic model
deploys. CI's dbt run writes them directly to the same lakehouse (duckrun adapter) before
`deploy.py --full` runs, so by the time step 5 deploys + refreshes the model the tables
are present. Order in `deploy.py`: lakehouse → notebook → variable library → copy dbt →
semantic model → pipeline (+ schedule).

## Delta writes via the duckrun dbt adapter

Tables are written as **Delta Lake** directly to OneLake by the `duckrun` dbt adapter
(DuckDB executes, delta-rs materializes). No Iceberg, no OneLake Iceberg→Delta
virtualization.

- **Adapter wiring** lives in `dbt/profiles.yml`: `type: duckrun`, with
  `root_path: {{ env_var('ONELAKE_TABLES_PATH') }}` and **no** storage token (the adapter
  auto-acquires the OneLake token via the same GitHub OIDC path).
  `ONELAKE_TABLES_PATH` = `abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables`.
- **Models persist** to `<root_path>/<schema>/<model>` as Delta tables, readable by
  Power BI Direct Lake immediately (no async metadata generation delay).
- **Incremental strategies** are real Delta operations: `merge` (upsert, needs
  `unique_key`), `insert` (idempotent append of new keys), `append`. This pipeline is
  **file-level incremental**: each fact model reads only files not already loaded
  (`... NOT IN (SELECT file FROM {{ this }})`) and writes with `incremental_strategy='insert'`
  keyed on the **filename** — idempotent, re-running a file is a no-op. `dim_duid` uses
  `merge` (a dimension whose attributes change; source is unique on `DUID`).
  `dim_calendar` is a one-off: `incremental`/`append` with `{% if is_incremental() %}WHERE 1=0{% endif %}`,
  so it builds in full on the first run and is a no-op afterward (dbt's "create if not exists, else skip").
  `fct_summary` uses `incremental_strategy='insert'` on `['date','time','DUID']` (dup-safe,
  insert-only — never blind `append`, never updates a key) for intraday rows, and
  **overwrite** when a new **daily** file arrives. The overwrite is duckrun-native:
  the model computes `has_new_daily` (before `config`) and sets `config(full_refresh=has_new_daily)`,
  so `_delta_core.sql` passes `full_refresh` to the delta-write plugin which rewrites the
  table fresh. duckrun has NO `overwrite` incremental strategy — overwrite IS `full_refresh`.
  Do NOT use a `DELETE FROM`/`TRUNCATE` pre_hook (`{{ this }}` is a `delta_scan` view, not
  writable), and do NOT switch to `materialized='table'` (that overwrites every run and
  loses the intraday-append optimization).
- **No DuckDB version pin** and no `force install iceberg/avro from core_nightly`.
  Only the `azure` extension is needed (for abfss CSV reads); duckrun bundles
  `dbt-duckdb` + `deltalake` and auto-creates the DuckDB Azure secret from the token.
