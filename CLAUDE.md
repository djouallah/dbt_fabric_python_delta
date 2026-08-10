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
  `ONELAKE_TABLES_PATH` is the **OneLake shorthand** `{ws_name}/{lh_name}.Lakehouse` — duckrun
  expands `<workspace>/<item>[/…]` to `abfss://{ws}@onelake.dfs.fabric.microsoft.com/{item}/Tables/…`
  at every seam a root enters (profile `root_path`, `duckrun.connect`, source `location`). The full
  abfss URL still works and means the same thing.
  - Anything that pastes a path into **raw SQL** never goes through the expander, so it must use an
    already-expanded value: `check_new_daily.sql` uses `target.root_path` (the adapter surfaces the
    expanded root on the Jinja `target`), and `FILES_PATH` stays a **full abfss URL** because
    `sources.yml` / `stg_csv_archive_log.py` feed it to `read_csv_auto` / `read_parquet` directly.
  - Shipped in duckrun **0.4.27**; CI installs plain `duckrun` from PyPI.
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
- **Hand-maintained reference data is a `table` model with an inline `VALUES` list, never
  a dbt seed.** Two reasons, both fatal to seeds here: duckrun materializes `seed` into
  in-memory DuckDB rather than Delta, so a seed is invisible to Power BI Direct Lake and
  to the Fabric Ontology/Graph items; and CI + the notebook both drive `dbt run`, which
  never executes seeds at all. `dim_region`, `dim_interconnector` and
  `dim_participant_parent` follow this pattern. Adding a `seeds/` directory would mean
  adding a `dbt seed` step to *both* runners and keeping them in parity — don't.
- **`Unit` is a reserved word in Fabric GQL.** A bare `(u:Unit)` fails as a *syntax*
  error, not "no such label", so it reads like the data binding broke. Backtick it:
  ``(u:`Unit`)``. See `gql.py`.
- **Bind `DOUBLE`, never `DECIMAL(p, s)`, to a Double ontology property.** The type map
  sends bare `decimal`/`double`/`float` to Double but a *parameterised* `decimal(p, s)`
  to **String** — so a `DECIMAL(18,2)` column bound to a Double property ingests as all
  NULL, silently, with no error anywhere. `agg_unit_daily` casts to `DOUBLE` for this
  reason; `fct_summary` keeps `DECIMAL` because it is not bound to the ontology.
- **Composite entity keys and duplicate property names DO work — verified in
  `ontology_v2.py`.** `entityIdParts` takes multiple property ids and a contextualization
  binds multiple key-ref columns: 49,245 UnitDay nodes keyed `[DUID, DateKey]` ingested
  with a two-column `PRODUCED` edge that traverses correctly (58,231.4 MWh test query
  matches v1/SQL exactly). Property names may also repeat across entity types when the
  value type matches (`Unit.Region` and `Station.Region` are both plain `Region`). So
  v1's concatenated `UnitDayKey` surrogate and global name prefixing were both
  unnecessary. Still true: key parts are String/Integer only — a date in the key must be
  an ISO string (`DateKey`), and a brand-new graph answers `GraphNotQueryable` (HTTP 400)
  until its first load completes.
- **TimeSeries-bound properties are INVISIBLE to GQL — by design (verified in
  `ontology_v3.py`).** Binding `fct_summary` to Unit as a TimeSeries data binding
  deploys fine, but `u.MW` fails with "Property 'MW' does not exist in type" and
  `Timestamp` is a GQL reserved word. The graph model only materializes non-timeseries
  properties and edges; time series are meant to be queried through a separate surface
  (KQL/Eventhouse, entity Overview widgets, or the Data Agent's NL2Ontology routing,
  which splits a question into GQL for structure + KQL for observations). So GQL alone
  can never aggregate a measure that lives in a time series — a graph-side MWh answer
  requires either an aggregate entity (the UnitDay pattern) or cross-engine routing.
- **A leaf-grain fact table CAN live in the graph (verified in `ontology_v4.py`):**
  11.28M Observation nodes (one per fct_summary row, three-part composite key
  `[DUID, DateKey, Interval]`) + 10.75M PRODUCED edges ingested in ~5 minutes, counts
  exact. `sum(o.MW)` over the AGL ownership traversal at 5-minute grain matches the SQL
  ground truth to the decimal. Costs: keyed lookups ~2-3s, the full rollup traversal
  ~37s (vs seconds in DuckDB); fact rows whose DUID is missing from dim_duid become
  edge-less orphan nodes (~530k); and the FIRST heavy query after a load can return
  status 00000 with EMPTY data — treat an empty aggregate result as suspect and retry.
- **Changed data needs an explicit graph refresh; only schema changes auto-refresh.**
  `POST /v1/workspaces/{ws}/items/{graphId}/jobs/instances?jobType=RefreshGraph`
  (the job type is undocumented — `Refresh`/`GraphRefresh` return `InvalidJobType`).
  `ontology.py` fires it on every run; it takes a couple of minutes to land.
- **Interconnector is a graph NODE, so a region-to-region hop is two edges** — and GQL
  rejects a quantified pattern spanning it ("Parenthesized path pattern expressions must
  be formed of exactly one edge pattern in between two node patterns"). `shutdown.py`
  therefore walks one hop at a time and closes in Python. Modelling the link as an edge
  would allow `{1,n}` but lose per-link filtering, because relationship instances carry
  no properties. GQL also has **no date functions or literals yet**, which is why
  `agg_unit_daily` exposes a `UnitDayKey` string.
- **GQL date ranges work via an ISO-date String property.** ISO-8601 strings order
  lexicographically, so `agg_unit_daily` carries `DateKey` (`CAST(date AS VARCHAR)`)
  and `WHERE ud.UnitDayDateKey >= '2026-08-03' AND ud.UnitDayDateKey <= '2026-08-09'`
  filters a week fine — no per-day key-equality OR chains needed.
- **GQL aggregation: no `round()`, and grouping must be an explicit `GROUP BY`.**
  Only `sum`/`count`/`max`-style aggregates parse; round client-side. Mixing a plain
  property with an aggregate in `RETURN` fails ("neither part of the GROUP BY nor an
  aggregation") — Cypher-style implicit grouping does not exist; write
  `RETURN ud.UnitDayDateKey AS day, sum(...) AS mwh GROUP BY day ORDER BY day`.
- **Adding a column to `dim_duid` needs the schema probe, not just the new-DUID probe.**
  The model short-circuits to `SELECT * FROM {{ this }} WHERE FALSE` when no new DUID
  appears, so a newly added column would stay NULL forever on the existing rows. It
  therefore also runs a `DESCRIBE` against `{{ this }}` and forces a full rebuild once
  when the column is absent. Extend that probe's column name when adding the next one.

## Running a deployed item on Fabric

`ws.run("run")` runs a deployed notebook (or pipeline) and waits for it — no `-i '{}'`
incantation. `deploy.py` deliberately does NOT run the notebook: CI's own dbt run already
wrote the Delta tables to the same lakehouse, so it would be redundant. The notebook is
exercised by the scheduled pipeline.
