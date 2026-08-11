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
  **overwrite** when a new **daily** file arrives. **The overwrite is decided by the RUNNER,
  not the model** — `full_refresh` is fixed at parse time (`execute=False`) so a run-time
  probe cannot toggle it, and the old `has_new_daily`/`config(full_refresh=…)` trick was
  always pinned to overwrite. Both runners (the notebook and CI) call the `check_new_daily`
  run-operation first, which raises `NEW_DAILY_PENDING`, and only then rerun
  `dbt run --select fct_summary+ --full-refresh`. duckrun has NO `overwrite` incremental
  strategy — overwrite IS `full_refresh`.
  `check_new_daily` **hardcodes the `landing/fct_scada` path** in raw SQL (raw SQL bypasses
  duckrun's shorthand expander, so it uses `target.root_path`); renaming or moving that table
  means editing the macro in lockstep.
  Do NOT use a `DELETE FROM`/`TRUNCATE` pre_hook (`{{ this }}` is a `delta_scan` view, not
  writable), and do NOT switch to `materialized='table'` (that overwrites every run and
  loses the intraday-append optimization).
- **The landing tables were always complete; the mart layer was the bottleneck.**
  `fct_price` is not a price table — it is AEMO's **DREGION** record, all 126 source columns
  ingested, including `TOTALDEMAND`, `NETINTERCHANGE`, `DISPATCHABLEGENERATION/LOAD` and
  `AVAILABLEGENERATION`. `fct_scada` is the full **DUNIT** record, all 49 columns, including
  `TOTALCLEARED` and `AVAILABILITY`. `fct_summary` exposed 4 of those ~175 columns, and
  everything downstream (Direct Lake, ontology, data agent) binds to `fct_summary` — so the
  whole analytical surface was 4 columns wide. Hence `fct_region`, `fct_unit_dispatch`,
  `fct_interconnector`, `fct_interconnector_derived` and `fct_constraint`. **No new ingest
  was needed for any of the daily ones** — they are projections of tables already landed.
- **`NETINTERCHANGE` is positive when a region is a net EXPORTER.** Verified by the identity
  that holds every interval to 0.01 MW: `DISPATCHABLEGENERATION − DISPATCHABLELOAD −
  NETINTERCHANGE = TOTALDEMAND`. Getting this backwards silently inverts every flow answer.
- **The NEM is a TREE, so every interconnector flow is recoverable from `NETINTERCHANGE`
  alone.** `QLD1–NSW1–VIC1–SA1` with `TAS1` off `VIC1`: n nodes, n−1 edges, so n−1 net
  injections determine all edge flows. That is what `fct_interconnector_derived` does, and it
  is the only way to get **historical** flows — the `PUBLIC_DAILY` archive contains no
  interconnector record at all (only `DUNIT`, `DREGION`, `DISPATCH.CASESOLUTION`,
  `DISPATCH.REGIONFCASREQUIREMENT`). Validation: the residual `sum(NETINTERCHANGE)` averages
  **+85.8 MW over the full 74,592 intervals** (median +80.4, max +369.7) — that is
  transmission loss. Cross-check: derived flows land exactly on the physical ratings —
  Basslink peaks at **478.0 MW** and SA1↔VIC1 at **±815.0**, to the megawatt.
  **There is a noise floor: the residual dips slightly negative on 531 intervals (0.71%),
  worst −19.4 MW, and never below −25.** Those dips cluster where interchange is smallest
  (the 500–1500 MW band), i.e. AEMO's per-region loss allocation rounding, not a broken
  invariant. So `assert_network_residual_is_loss.sql` bounds each interval at **−50 MW**
  (≈2.5× the worst observed noise) and separately asserts the **mean is positive**. A first
  version asserting `>= −1` per interval failed on 421 rows of ordinary noise. The two checks
  target the two real failures: a sign flip drags the mean to −86 (verified: 55,007 rows
  returned), and **EnergyConnect (PEC, SA1↔NSW1) commissioning** closes a cycle and breaks
  the balance by hundreds of MW (verified: a simulated 300 MW uncounted link returns 74,295
  rows). If it fires for the second reason, **retire `fct_interconnector_derived` in favour of
  `fct_interconnector`** — do not patch it.
- **The intraday DISPATCHIS files carry six record types beyond the one being read.**
  `fct_price_today`'s `WHERE I='D' AND PRICE='PRICE'` filter was discarding
  `INTERCONNECTORRES` (per-link `MWFLOW`, `EXPORTLIMIT`, `IMPORTLIMIT` — real transfer
  limits), `CONSTRAINT` (1,053 rows/interval: which limits bound and their shadow prices),
  `REGIONSUM`, `LOCAL_PRICE`, `INTERCONNECTION` and `CASE_SOLUTION`. `fct_interconnector_today`
  and `fct_constraint_today` read the **same archive files** with a different record filter —
  no new download, no change to `stg_csv_archive_log.py`. Each record type has a different
  width, so each needs its own `columns={...}` list and its own `RECORD = '…'` filter.
  These are **intraday-only with no back-history**: DISPATCHIS is a Current report on a
  rolling ~2-day window, so the tables start empty and accumulate forward. That is AEMO's
  publishing, not a bug.
- **`fct_unit_dispatch` deliberately does NOT filter `INITIALMW <> 0`**, unlike `fct_summary`.
  A unit that offered capacity and was not dispatched is the most valuable row in the table —
  it is what makes headroom (`AVAILABILITY − INITIALMW`) a measured number instead of one
  inferred from observed peaks. Measured: on one QLD coal day, 2,592 of 8,064 rows are
  undispatched, and average `AVAILABILITY` (250 MW) exceeds average `INITIALMW` (169 MW).
  Cost is ~3× `fct_summary`'s row count.
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
- **`Unit` and `Interval` are reserved words in Fabric GQL.** A bare `(u:Unit)` or a bare
  `o.Interval` fails as a *syntax* error, not "no such label/property", so it reads like
  the data binding broke. Backtick both — ``(u:`Unit`)``, ``o.`Interval` `` — which works
  in `RETURN`, `WHERE` and `GROUP BY` alike. Note `ontology_v4.py` renamed `time` →
  `Interval` specifically to dodge `TIME` being reserved, and landed on another reserved
  word; the binding is fine, only unbackticked references fail. See `gql.py`.
- **`fct_summary.time` (and so `Observation.Interval`) is HHMM, not minutes past midnight.**
  `CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT)` → `0, 5, … 55, 100, 105, … 2355`; min 0,
  max 2355, 288 distinct. Decode as `hour = time / 100`, `minute = time % 100`. The values
  are deliberately **not** evenly spaced (nothing between 55 and 100), so never treat it as
  a duration — but it does compare numerically, so a clock window is a plain
  `BETWEEN 1800 AND 2000`. An earlier note here claimed minutes-past-midnight; that was
  wrong, and only looked right because the first hour (0, 5, 10, 15) is identical either way.
- **Bind `DOUBLE`, never `DECIMAL(p, s)`, to a Double ontology property.** The type map
  sends bare `decimal`/`double`/`float` to Double but a *parameterised* `decimal(p, s)`
  to **String** — so a `DECIMAL(18,2)` column bound to a Double property ingests as all
  NULL, silently, with no error anywhere. **Every mart casts measures to `DOUBLE`** for this
  reason — `fct_summary` included (it emits `DOUBLE`, not `DECIMAL`, despite an earlier note
  here saying otherwise), and so do `fct_region`, `fct_unit_dispatch`, `fct_interconnector`,
  `fct_interconnector_derived` and `fct_constraint`.
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
- **A Data Agent is deployable code-first; its API mirrors `/ontologies`.** `data_agent.py`
  lists/creates/updates `{API}/workspaces/{ws}/dataAgents` with base64 definition parts
  exactly like the ontology scripts, then **`POST .../dataAgents/{id}/staging/publish`** —
  publishing is NOT optional, because the MCP endpoint `ask.py` queries
  (`{API}/mcp/workspaces/{ws}/dataagents/{id}/agent`, note the **lowercase** `dataagents`
  there vs `dataAgents` on the management path) errors until the agent is published once.
  `getDefinition` is an LRO whose payload is at `{Location}/result`, not in the
  operation-status body, so the shared `call()` helper is not enough for it.
- **The Ontology data source is STRICTER than raw GQL against the GraphModel.** Queries
  `gql.py` runs happily via `/GraphModels/{id}/executeQuery` are rejected when the Data
  Agent runs them through the ontology: returning a bare `o.MW` / `o.Price` per row fails
  with *"The field 'Price' is not configured for time series data"* — an error about the
  query SHAPE, not the binding. Wrap the measure in `sum`/`avg`/… with an explicit
  `GROUP BY` and the same field works. So `aiInstructions` must forbid un-aggregated
  Observation returns outright.
- **A comma-joined multi-pattern `MATCH` over Observation is a fan-out, not a filter.**
  `MATCH (u:`Unit`)-[:PRODUCED]->(o:Observation), (u)-[:PART_OF]->(s:Station), …` inflated
  one day of SA1 from 15,032 readings to 2,076,155 (138×) and the answer with it. Filter on
  `Unit`'s own `Region` property instead of traversing to Station→Region.
- **Measure answers from the Data Agent are NOT trustworthy; structural answers are.**
  This is the honest verdict of `ask.py`. Ownership/topology questions (AGL's capacity incl.
  subsidiaries, Basslink islanding TAS1, gen+load stations, shared SA/VIC owners, top-10 by
  capacity) come back correct and repeatable. Aggregations over the 11M `Observation` nodes
  do not: the same question answered **exactly** right once (SA1 2026-08-09 = $35.145 from
  15,032 readings / 66 units, and 46,351.5 MWh) and then, unprompted, **$111.12 from
  1,329,227 readings / 80 units** — an 88× fan-out — on a later run, with the instruction
  forbidding extra MATCH patterns still in place. It has also silently reported `sum(o.MW)`
  as MWh (556,218 vs 46,352 — the 5/60 factor dropped). Instructions reduce these but do
  not eliminate them, because the fan-out is the NL2Ontology planner's choice, not the
  prompt's. **Mitigation that actually works:** require every measure answer to also return
  `count(o.MW)` and `count(DISTINCT u.DUID)` in the same query. It does not prevent the bad
  number, but it makes it visible — an inflated scope is the tell, and without it the wrong
  number arrives stated confidently as fact.
- **`updateDefinition?updateMetadata=true` requires a `.platform` part** (`InvalidInput:
  "UpdateMetadata is true but .platform file was not provided"`). The ontology scripts ship
  one so the flag is fine there; the Data Agent definition has no `.platform` part, so
  `data_agent.py` must omit the flag.
- **The Data Agent is not deterministic, and it degrades under back-to-back load.** The
  same question against the same published agent answers correctly on one run and returns
  "a technical issue" (or a bare HTTP 500) on the next. Asking all nine `ask.py` questions
  in a row dropped it to 4/9, including questions that had just passed; "top 10 stations by
  registered capacity" failed in the batch and passed immediately when asked with spacing.
  `ask.py` therefore sleeps 20s between questions and retries once, printing the retry
  rather than hiding it — needing a retry is a quality signal, not noise.
- **An ontology data source has ONE tuning surface: `aiInstructions`.** Unlike a `graph`
  data source, an ontology source accepts no `fewshots.json` and no data-source
  instructions — so the schema map, glossary, GQL dialect rules and worked question→GQL
  exemplars all have to be inlined into that single string in
  `Files/Config/draft/stage_config.json`. Two non-obvious requirements: the instructions
  must literally contain **`Support group by in GQL`** (Microsoft's documented workaround
  for an aggregation known-issue — without it grouped questions fail), and the datasource
  folder name is literally `{dataSourceType}-{dataSourceName}`, which must agree with the
  `type` field inside `datasource.json`. The documented `type` enum
  (`lakehouse_tables`/`data_warehouse`/`kusto`/`semantic_model`/`graph`/…) does **not** list
  an ontology value, so `data_agent.py` bets on `"ontology"` and echoes back what Fabric
  actually stored; `python data_agent.py --dump [name]` decodes any agent's stored
  definition when that bet needs checking against a portal-built agent.
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
  `sum`/`count`/`min`/`max` **and `avg`** all parse (verified against v4 — an earlier note
  here claimed only sum/count/max); there is still no `round()`, so round client-side.
  Mixing a plain
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
