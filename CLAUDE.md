# Fabric Deploy — Lessons Learned

`deploy.py` deploys every Fabric item through the **duckrun workspace API** — no `fab` CLI,
no `parameter.yml`. It is a flat script: constants at the top, statements top-to-bottom.
GitHub Actions is the real orchestrator; the notebook + pipeline are the in-Fabric demo.

## One folder deploy does everything

`fabric_items/` is the Fabric git-integration layout (`<name>.<ItemType>/` each with a
`.platform`). `ws.deploy("fabric_items", ...)` ships the whole tree **in dependency order** —
Variable Library → Notebook → Semantic Model → Data Pipeline — and returns `{displayName: id}`:

- **Names come from each `.platform`** — no `name=` and no folder-name parsing needed.
- **`lakehouse=`** repoints the Direct Lake `model.bim`'s OneLake workspace/lakehouse GUIDs and
  **refreshes** the model. The checked-in bim keeps its original dev GUIDs; never hand-edit them.
- **Pipeline notebook activities are auto-wired** to the folder's notebook when there is exactly
  one (ours is `run`). No patching `notebookId`/`workspaceId` by index. Pass `notebook=` only if
  the folder ever gains a second notebook.
- **`variables={...}`** injects the env-specific values into the `deploy_config` Variable Library
  at deploy time — no editing `variables.json` and reverting it afterwards.
- **`folder=`** places items in a workspace folder, but **only when they are CREATED**. An
  `overwrite` of an existing item updates it in place and leaves it where it lives, so items that
  predate the folder must be moved (or deleted and redeployed) once.
- **`ws.schedule(name, every="720m")`** is idempotent — it updates the existing schedule rather
  than stacking duplicates. No list/dedup logic.

## Auth: OIDC only, no az login and no token step

duckrun mints every token it needs (OneLake storage, Fabric control plane, Power BI) by
exchanging a fresh GitHub OIDC assertion. CI needs only `id-token: write` plus
`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` in the env. `dbt/profiles.yml` carries **no**
`storage_options` token — the adapter self-acquires.

Two CI-only env vars are still required for DuckDB's OneLake reads (its azure extension's
default transport fails the OneLake TLS handshake):

```yaml
AZURE_TRANSPORT_OPTION_TYPE: curl
CURL_CA_INFO: /etc/ssl/certs/ca-certificates.crt
```

Historical note: a pure-OIDC run used to fail with a bare `Unauthorized` because the OIDC token
*fetch* could time out and duckrun swallowed the error (duckrun#10 — the token itself was always
valid, returning 200 on both the dfs and blob endpoints). Fixed in duckrun 0.4.26, which retries
the exchange and propagates the failure instead of swallowing it.

## Config lives in deploy.py

There is no `deploy_config.yml`. `WORKSPACE`, `LAKEHOUSE`, `FOLDER`, `SCHEDULE_EVERY`,
`DOWNLOAD_LIMIT`, `PROCESS_LIMIT` are constants at the top of `deploy.py`. The workflow repeats
`WS_ID` / `LH_NAME` in its own `env:` block (deploy.py is a flat script, so importing it to read
the constants would run a deploy) — **keep the two in sync**.

## Delta writes via the duckrun dbt adapter

Tables are written as **Delta Lake** directly to OneLake by the `duckrun` dbt adapter
(DuckDB executes, delta-rs materializes). No Iceberg, no OneLake Iceberg→Delta
virtualization.

- **Adapter wiring** lives in `dbt/profiles.yml`: `type: duckrun`, with
  `root_path: {{ env_var('ONELAKE_TABLES_PATH') }}` and **no** `storage_options` token — the
  adapter self-acquires the OneLake token (Fabric runtime / GitHub OIDC / `az login`).
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

## Running a deployed item on Fabric

`ws.run("run")` runs a deployed notebook (or pipeline) and waits for it — no `-i '{}'`
incantation. `deploy.py` deliberately does NOT run the notebook: CI's own dbt run already
wrote the Delta tables to the same lakehouse, so it would be redundant. The notebook is
exercised by the scheduled pipeline.
