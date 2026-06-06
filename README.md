# ⚠️ HIGHLY EXPERIMENTAL — MAY KILL A DUCK 🦆💀

> [!CAUTION]
> **This is not for production systems. Experimental and educational purposes only.**

---

# dbt + DuckDB + OneLake Delta (via duckrun)

The whole pipeline runs anywhere Python runs — your laptop, a GitHub Actions runner, a
container, an AI agent. Transformations are written as dbt models; [**duckrun**](https://github.com/djouallah/duckrun)
is the dbt adapter that executes them with DuckDB and materializes the results as
**Delta Lake** tables directly in OneLake (via delta-rs) — no Spark, no Iceberg, no
async virtualization. Delta is read natively by Power BI Direct Lake.

> **DuckDB executes · delta-rs materializes · dbt orchestrates.**

![OneLake explorer showing the data lakehouse with mart schema tables and dbt project files](onelake.png)

Concretely, for OneLake:
- `ONELAKE_TABLES_PATH` = `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}.Lakehouse/Tables`
- `FILES_PATH` = `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files`
- `TOKEN` = bearer token from `notebookutils.credentials.getToken('storage')` (in Fabric) or `az login --scope https://storage.azure.com/.default` (locally — `AzureCliCredential` picks it up automatically, no secrets to manage)

Use the IDs, not the names. `deploy_config.yml` stores the workspace GUID under `ws`; the
workspace display name is resolved at deploy time via `fab api -X get workspaces/{ws}`.
Inside Fabric the notebook resolves the lakehouse GUID via
`notebookutils.lakehouse.get(lakehouse_name).id`; outside Fabric the `local` section of
`deploy_config.yml` holds both GUIDs directly.

You can run the notebook anywhere — laptop, GitHub, Colab — but running inside Fabric
gives you in-region latency, no egress, a scheduler, and automatic token handling.

### Limitations

- **Direct Lake reads** the Delta tables natively — duckrun writes real Delta, so there
  is no Iceberg→Delta virtualization step and no metadata-generation delay.
- **Table maintenance is on you** — Delta compaction (`OPTIMIZE`) and vacuum. `deltalake`
  (delta-rs) is a reasonable place to start.

## dbt duckrun configuration

In `profiles.yml`, all targets use the `duckrun` adapter pointed at the OneLake
`Tables/` path:

```yaml
aemo_electricity:
  target: dev
  outputs:
    dev:
      type: duckrun
      schema: "{{ env_var('DBT_SCHEMA', 'mart') }}"
      root_path: "{{ env_var('ONELAKE_TABLES_PATH') }}"
      storage_options:
        bearer_token: "{{ env_var('ONELAKE_TOKEN') }}"
      settings:
        preserve_insertion_order: false
```

The adapter auto-creates a matching DuckDB Azure secret from the bearer token, enabling
`delta_scan()` reads. Models persist to `<root_path>/<schema>/<model>` as Delta tables.

### Incremental strategies

Delta supports real upserts, so every incremental model uses `merge`:

| Strategy | Behavior | Requires |
|----------|----------|----------|
| `merge` | Upsert — update matched rows, insert new | `unique_key` |
| `insert` | Insert only new keys (idempotent) | `unique_key` |
| `append` | Blind append | — |

```sql
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='date') }}
```

## Schema layout

- **`landing`** — staging and incremental fact tables (source ingestion, deduplication): `fct_scada`, `fct_price`
- **`mart`** — Power BI-facing models (joined, aggregated, ready for Direct Lake): `dim_duid`, `fct_summary`

---

## Manual deploy from laptop using Fabric CLI

```bash
az login
python deploy.py --env main
```

All you need:
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) — `az login` uses your own identity, whatever access you have in Fabric is what the deploy gets
- [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) (`pip install ms-fabric-cli`)

No service principal, no app registration, no secrets — that whole song-and-dance is only for the CI path below.

## Optional: automated deployment to Fabric

Everything below is opt-in. The core dbt + DuckDB + OneLake loop runs without any of it.
This section covers using the included `deploy.py` + GitHub Actions to provision a
Lakehouse, push the notebook, run dbt on a schedule, and refresh a Power BI semantic model.

### Architecture

```
GitHub Push
    │
    ▼
GitHub Actions CI
    ├── dbt run   (DuckDB + duckrun → OneLake Delta, test workspace)
    └── dbt test  (validates Delta table row counts)
    │
    ▼
deploy.py (Fabric CLI + Power BI API)
    ├── fab create  → Lakehouse (with schemas)
    ├── fab deploy  → Notebook
    ├── Copy dbt/   → OneLake Files
    ├── fab job run → Notebook runs dbt, writes Delta tables to OneLake
    ├── fab deploy  → Semantic Model (Direct Lake, GUIDs swapped)
    ├── Power BI API → Refresh semantic model
    └── fab deploy  → Data Pipeline + cron schedule
```

![Fabric workspace after deploy: semantic model, lakehouse, variable library, notebook, and pipeline](items.png)

### Stack

| Layer | Tool |
|-------|------|
| Transformations | dbt-core + duckrun (dbt-duckdb adapter) |
| Table format | Delta Lake (delta-rs / `deltalake`) |
| Execution | Python notebook (Fabric) |
| Storage | OneLake (Delta / Parquet) |
| Serving | Direct Lake semantic model (Power BI) |
| CI | GitHub Actions |
| Deploy | Fabric CLI (`ms-fabric-cli`) |

### Environments

| Target | Warehouse | Token source | Use case |
|--------|-----------|--------------|----------|
| `dev` | Test workspace on OneLake | `az login` (AzureCliCredential) | Local development |
| `ci` | Test workspace on OneLake | OIDC federated credential (no stored secret) | GitHub Actions |
| `prod` | Prod workspace on OneLake | notebookutils | Fabric notebook |

### Configuration files

- `deploy_config.yml` — workspace ID, schedule, and settings per environment
- `profiles.yml` — dbt targets with the duckrun adapter config
- `dbt_project.yml` — model config and variable defaults

### CI/CD setup (GitHub Actions)

Auth is **OIDC** — no long-lived bearer tokens stored in GitHub. The workflow exchanges
GitHub's short-lived OIDC token for an Azure AD federated credential via `azure/login@v2`,
then mints OneLake storage tokens at runtime with `az account get-access-token`. Tokens
live only for the duration of a job.

The only GitHub secrets you need:
- `AZURE_CLIENT_ID` — your Azure AD app registration
- `AZURE_TENANT_ID` — your tenant

On the Azure side, register an app and add a **federated credential** with subject
`repo:<owner>/<repo>:ref:refs/heads/main` (and one per deploy branch). Grant it the Fabric
workspace permissions you need.

Push to `main` runs CI tests and publishes dbt docs to GitHub Pages. Push to `production`
deploys to Fabric.

---

## Learnings

- **Emit `timestamptz`, not `timestamp`.** Naive `TIMESTAMP` maps to Delta `timestamp_ntz`,
  which Microsoft docs flag as "not fully supported across Fabric workloads."
  `CAST(... AS TIMESTAMPTZ)` at output columns.
- **Use `merge` for incrementals.** Delta supports MERGE/DELETE natively, so models
  upsert by `unique_key` — no append-only workarounds, no DELETE pre_hook hacks.
- **Delta maintenance is on you.** Compaction (`OPTIMIZE`) and `VACUUM` via delta-rs.
