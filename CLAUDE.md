# Fabric CLI Deploy — Lessons Learned

## parameter.yml

### replace_value token resolution
`extract_replace_value` only resolves `$workspace.id` / `$items.*` if the **entire string starts with `$`**.
Embedding tokens inside a URL string does NOT work:
```yaml
# WRONG — $workspace.id is never resolved
replace_value:
  _ALL_: "https://onelake.dfs.fabric.microsoft.com/$workspace.id/$items.Lakehouse.data.$id/"

# CORRECT — replace each GUID separately so replace_value starts with $
- find_value: "e446a5e7-6666-42ad-a331-0bfef3187fbf"
  replace_value:
    _ALL_: "$workspace.id"
- find_value: "cf8bbf8a-5488-4020-ac8f-293727b447b1"
  replace_value:
    _ALL_: "$items.Lakehouse.data.$id"
```

### Tokens are ONLY resolved through parameter.yml
Fabric CLI does NOT resolve `$workspace.id` or `$items.*` tokens inline in content files.
Content files (e.g. pipeline-content.json) must contain the original dev GUIDs, and
parameter.yml must have find_replace entries to swap them with `$` tokens.
```json
// WRONG — literal token strings end up deployed unresolved
{ "workspaceId": "$workspace.id", "notebookId": "$items.Notebook.run.$id" }

// CORRECT — use dev GUIDs, let parameter.yml replace them
{ "workspaceId": "e446a5e7-6666-42ad-a331-0bfef3187fbf", "notebookId": "da888b35-..." }
```

### $items token format
Required format: `$items.type.name.$attribute`
```yaml
$items.Lakehouse.data.$id    # correct — type=Lakehouse, name=data
$items.Notebook.run.$id      # correct — type=Notebook, name=run
$items.data.$id              # wrong — "Invalid $items variable syntax"
```

### is_regex must be a string
```yaml
is_regex: "true"   # correct
is_regex: true     # wrong — Fabric CLI rejects with "not of type string"
```

### _ALL_ is the correct universal environment key
```yaml
replace_value:
  _ALL_: "$workspace.id"   # applies to any target environment
```

## fab deploy config YAML

### Key is item_types_in_scope (plural)
```yaml
item_types_in_scope:   # correct (plural)
  - Notebook
  - Lakehouse

item_type_in_scope:    # wrong (singular) — silently ignored, deploys everything
  - Notebook
```

## Deploy order for Direct Lake semantic models

Direct Lake validation requires Delta tables to exist before the semantic model is deployed.
Split into two phases using two config files:

**fab_deploy.yml** — Notebook + Lakehouse
**fab_deploy_sm.yml** — SemanticModel + DataPipeline

Deploy sequence in deploy.py:
1. `fab deploy` Lakehouse (must exist before Notebook so `$items.Lakehouse.data.$id` resolves)
2. `fab deploy` Notebook (now the Lakehouse token resolves)
3. Copy dbt files to OneLake
3. `fab job run prod.Workspace/run.Notebook -i '{}'` — runs notebook synchronously, creates Delta tables
4. `fab deploy --config fab_deploy_sm.yml` (SemanticModel + DataPipeline)
5. Schedule pipeline if not already scheduled

## Delta writes via the duckrun dbt adapter

Tables are written as **Delta Lake** directly to OneLake by the `duckrun` dbt adapter
(DuckDB executes, delta-rs materializes). No Iceberg, no OneLake Iceberg→Delta
virtualization.

- **Adapter wiring** lives in `dbt/profiles.yml`: `type: duckrun`, with
  `root_path: {{ env_var('ONELAKE_TABLES_PATH') }}` and `storage_options.bearer_token`.
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
  `fct_summary` keeps its original incremental design: `append` intraday rows each run,
  and **overwrite** only when a new **daily** file arrives. The overwrite is duckrun-native:
  the model computes `has_new_daily` (before `config`) and sets `config(full_refresh=has_new_daily)`,
  so `_delta_core.sql` passes `full_refresh` to the delta-write plugin which rewrites the
  table fresh. duckrun has NO `overwrite` incremental strategy — overwrite IS `full_refresh`.
  Do NOT use a `DELETE FROM`/`TRUNCATE` pre_hook (`{{ this }}` is a `delta_scan` view, not
  writable), and do NOT switch to `materialized='table'` (that overwrites every run and
  loses the intraday-append optimization).
- **No DuckDB version pin** and no `force install iceberg/avro from core_nightly`.
  Only the `azure` extension is needed (for abfss CSV reads); duckrun bundles
  `dbt-duckdb` + `deltalake` and auto-creates the DuckDB Azure secret from the token.

## fab job run for notebooks
Notebooks require `-i '{}'` (empty input JSON), otherwise the command does nothing:
```bash
fab job run prod.Workspace/run.Notebook -i '{}'   # correct
fab job run prod.Workspace/run.Notebook            # does nothing for notebooks
```
