# Fabric Deploy — Lessons Learned

`deploy.py` deploys every Fabric item through the **duckrun workspace API** — no `fab` CLI,
no `parameter.yml`. It is a flat script: constants at the top, statements top-to-bottom.
GitHub Actions is the real orchestrator; the notebook + pipeline are the in-Fabric demo.

## One folder deploy does everything

`fabric_items/` is the Fabric git-integration layout (`<name>.<ItemType>/` each with a
`.platform`). `ws.deploy("fabric_items", ...)` ships the whole tree **in dependency order** —
Variable Library → Notebook → Data Pipeline — and returns `{displayName: id}`:

- **Names come from each `.platform`** — no `name=` and no folder-name parsing needed.
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

**The semantic model lives in `semantic_model/`, NOT `fabric_items/`, because duckrun's
`deploy()` takes no exclude filter.** A single `fabric_items/` call is all-or-nothing, so the
only way to make the model optional was to give it its own folder and its own gated call,
behind `DEPLOY_SEMANTIC_MODEL` (env var — NOT `sys.argv`, which `data_agent.py` scans for
`--dump` under `runpy`). The workflow's `deploy` input drives it: `no_model` exports
`DEPLOY_SEMANTIC_MODEL=false`. Consequences worth knowing:

- **`lakehouse=` now sits only on the `semantic_model` call.** It was always a no-op on the
  other three item types (duckrun forwards it and ignores it for anything that isn't a Direct
  Lake `.bim`). It still repoints the bim's OneLake workspace/lakehouse GUIDs and **refreshes**
  the model — the checked-in bim keeps its original dev GUIDs; never hand-edit them.
- **The pipeline auto-wiring survives** because `fabric_items/` still has exactly one notebook.
  Do NOT "fix" this by looping `ws.deploy` over the individual item folders instead — a
  per-item scan can't see the notebook list, so it would force a hardcoded `notebook="run"`
  and a hand-maintained item list, losing the `.platform`-derived names.
- **The pipeline now deploys BEFORE the model** (one sorted pass used to put the model third).
  Nothing depends on that order — `_DEPLOY_ORDER` exists for the notebook wiring, and no item
  references the model. The pipeline has only `TridentNotebook` activities, no `RefreshDataset`.
- **Skipping the model is safe** because nothing downstream binds to it: the ontology and data
  agent read the mart **Delta tables**. It is also the slow part of a deploy — the Direct Lake
  reframe blocks until it lands — so `no_model` is the fast loop for ontology work.
- **A `no_model` run leaves the deployed model in place**, just stale. Nothing to clean up; a
  later `full` run redeploys and refreshes it.

**The ontology and data agent deploy from `deploy.py` too, just not from `fabric_items/`.**
duckrun's folder deploy only knows `VariableLibrary`, `Notebook`, `SemanticModel` and
`DataPipeline`. `_scan_item_folders` validates **every** item folder before deploying **any** of
them and raises `unsupported item type` rather than skipping, so a single `.Ontology` folder
dropped into `fabric_items/` makes `ws.deploy("fabric_items", ...)` fail outright and ship
nothing — not the notebook, not the pipeline, not the variable library. (Tried it; that is
measured, not theoretical. There is no exclude filter, the same constraint that put the semantic
model in its own folder. Tracking issue:
[duckrun#57](https://github.com/djouallah/duckrun/issues/57).) Instead `deploy.py` ends with
`runpy.run_path("ontology.py")` then `runpy.run_path("data_agent.py")` — same step, same
`aemo` folder as everything else, and they run under **both** `deploy=full` and
`deploy=no_model` (only `none` skips them). Order matters: the agent binds the ontology by
displayName, so the ontology has to exist first. Nothing about them runs locally or on a
plain push.

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

## Config: one env vocabulary, nothing hardcoded twice

There is no `deploy_config.yml`, and since plan B there are no per-script constants to keep
in sync either. The deploy target is four env vars — `WS_ID`, `LH_NAME`, `LH_LANDING`,
`FOLDER` — surfaced as `workflow_dispatch` inputs in `pipeline.yml` (defaults:
`450bf196-431f-463f-9316-2d1ce1da98db` = `sqlengines`, `aemo`, `aemo_landing`, `FabricIQ`)
and read with the SAME defaults by every script (`deploy.py`, `ontology.py`,
`data_agent.py`, `refresh_graph.py`, `download.py`, `ask.py`, `gql.py`, `shutdown.py`,
`operations_agent.py`, and the notebook's laptop-fallback branch). CI inputs flow through
the job `env:` into `deploy.py` and everything it `runpy`s; a bare laptop run targets the
same place as a bare `gh workflow run`. `SCHEDULE_EVERY` / `DOWNLOAD_LIMIT` stay constants
in `deploy.py`. To deploy the stack to ANY workspace, pass `-f workspace_id=…` (plus
lakehouse/folder names if desired) — that is the whole procedure.

**The old `analytics_as_code` stack (East US) is frozen, not migrated.** Its items, its
8-year archive in `data_landing`, and its own 720m schedule are all still there and
deliberately untouched; the parameterized default now points elsewhere, so CI no longer
builds into it. The move happened because East US excludes the Operations Agent item type
(see below).

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
    already-expanded value. `FILES_PATH` therefore stays a **full abfss URL** — the landing
    pre_hooks and `stg_csv_archive_log.py` feed it to `read_csv` / `read_parquet` directly.
  - Shipped in duckrun **0.4.27**; CI installs plain `duckrun` from PyPI.
- **Models persist** to `<root_path>/<schema>/<model>` as Delta tables, readable by
  Power BI Direct Lake immediately (no async metadata generation delay).

## Two lakehouses, and NO incremental

**The landing lakehouse (`LH_LANDING`, default `aemo_landing`) holds the raw AEMO archive;
the transform lakehouse (`LH_NAME`, default `aemo`) holds every Delta table dbt builds**,
plus the dbt project under `Files/dbt`. Since plan B **both are created idempotently** by
CI Phase 1 and `deploy.py` — a fresh workspace starts with an EMPTY landing and
`stg_csv_archive_log.py` bootstraps it from scratch (missing log → empty temp table →
downloads proceed, GitHub backfill included), `download_limit` files per source per run.
The archive therefore grows gradually; the first runs build small marts, and the dbt tests
are invariants that hold at any volume (measured on the very first scratch run: 68 pass,
1 warn — the known orphan-DUID relationship warn).

Historical note: the old workspace's `data_landing` (the ORIGINAL `data` lakehouse renamed
by hand — same GUID, so `6bd45441-…` points at the archive, not the mart) holds the full
8-year, ~1,060-file archive. It is frozen with the rest of that stack, and the old
"fail loudly rather than provision an empty landing" guard went with it — scratch
bootstrapping is now the designed behaviour, not an accident to prevent.

The split is two env vars pointing at two lakehouses — nothing more:

| var | lakehouse | form |
| --- | --- | --- |
| `ONELAKE_TABLES_PATH` | `LH_NAME` (`aemo`) | shorthand in CI, full abfss in the notebook |
| `FILES_PATH` | `LH_LANDING` (`aemo_landing`) | always a full abfss URL |

There is **no OneLake shortcut and no `sources.yml` with `location:`**. dbt keeps ONE
`root_path`, so every `ref()` resolves inside one lakehouse and no model crosses — only the
raw `read_csv` paths point elsewhere, and those were already absolute strings. The dbt
project itself (`Files/dbt`) lives in the transform lakehouse, which the notebook mounts.

**Every model is `materialized='table'`. There is no `is_incremental()` anywhere.** The data
was deleted and rebuilt from scratch, and incremental had nothing left to preserve. What went
with it, so nobody reintroduces it piecemeal:

- `dbt/macros/check_new_daily.sql`, the `NEW_DAILY_PENDING` signal, and the `--full-refresh`
  branch in both runners. **CI Phase 2 and the notebook are now a single `dbt run`** — the DAG
  order is all the sequencing there is.
- The `full_refresh` workflow input and `PROCESS_LIMIT`/`process_limit` everywhere. That limit
  was a `LIMIT n` inside each landing pre_hook; under a full rebuild it silently TRUNCATES the
  build to n files, which is why it had to go rather than being raised.
- The `has_files` / `has_new_duids` probes and their `SELECT * FROM {{ this }} WHERE FALSE`
  fallbacks — a table model has no `{{ this }}` on the first run, so those were actively unsafe.
- `dim_duid`'s `DESCRIBE`-based schema-drift probe, and `dim_calendar`'s
  `{% if is_incremental() %}WHERE 1=0{% endif %}` idiom.
- `fct_summary` was two queries joined by `{% if is_incremental() %}`; it is now just the
  full-rebuild branch, and the `cutoff` watermark column that existed only to serve the append
  path is gone.

`download_limit` **stays** — it caps how many NEW files to fetch per run. Downloads are still
incremental, but the state is `csv_archive_log.parquet` in `Files/`, not a Delta table, which
is also why `stg_csv_archive_log` can be a plain `table` model.

## Column naming convention

1. **A column referencing an entity carries that entity's key name**, role-prefixed when there
   is more than one: `RegionID`, `FromRegionID`, `ToRegionID`.
2. **A mart column is named for the ontology property it binds to**, so every binding in
   `ontology.yaml` is an identity map (`MW: [MW, Double]`). The four genuine foreign keys are
   the only non-identity binds left, and they now read as what they are —
   `[[FromRegionID, RegionID]]`.
3. **Landing tables keep AEMO's names verbatim.** `fct_price` is AEMO's DREGION record and
   `fct_scada` its DUNIT record; renaming those would break the tie to AEMO's spec.

Applied renames: `time`→`TimeHHMM`, `mw`→`MW`, `price`→`Price`,
`FuelSourceDescriptor`→`FuelSource`, `TechnologyType`→`Technology`, `Region`→`RegionID`
(in `dim_duid`/`dim_station`), `FromRegion`/`ToRegion`→`FromRegionID`/`ToRegionID`. The
matching **ontology properties** moved too, so `model.bim`, `ontology.yaml`, `data_agent.py`'s
schema map and the `u.RegionID` filters in `gql.py`/`shutdown.py` all changed in lockstep.
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
  in `RETURN`, `WHERE` and `GROUP BY` alike. **v5 sidesteps this entirely** by naming the
  entities `GeneratingUnit` and `TimeHHMM`, so nothing in `ontology.py`, `gql.py` or
  `shutdown.py` needs a backtick today — but the trap is still live for any new entity or
  property you name, and the failure mode reads like a broken binding rather than a syntax
  error. (History: an earlier version renamed the `time` column to `Interval` specifically
  to dodge `TIME` being reserved, and landed on another reserved word.)
- **`fct_summary.TimeHHMM` (and so `Observation.TimeHHMM`) is HHMM, not minutes past midnight.**
  `CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT)` → `0, 5, … 55, 100, 105, … 2355`; min 0,
  max 2355, 288 distinct. Decode as `hour = TimeHHMM / 100`, `minute = TimeHHMM % 100`. The values
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
- **Composite entity keys and duplicate property names DO work** (verified in the v2
  experiment; `ontology.py` relies on it for all three fact entities).
  `entityIdParts` takes multiple property ids and a contextualization
  binds multiple key-ref columns: 49,245 UnitDay nodes keyed `[DUID, DateKey]` ingested
  with a two-column `PRODUCED` edge that traverses correctly (58,231.4 MWh test query
  matches v1/SQL exactly). Property names may also repeat across entity types when the
  value type matches (`GeneratingUnit.RegionID` and `Station.RegionID` are both plain `RegionID`). So
  v1's concatenated `UnitDayKey` surrogate and global name prefixing were both
  unnecessary. Still true: key parts are String/Integer only — a date in the key must be
  a `DateKey` integer, and a brand-new graph answers `GraphNotQueryable` (HTTP 400)
  until its first load completes.
- **TimeSeries-bound properties are INVISIBLE to GQL — by design** (the v3 dead end; this
  is why no entity in `ontology.py` uses `timeseriesProperties`).
  Binding `fct_summary` to a unit entity as a TimeSeries data binding
  deploys fine, but `u.MW` fails with "Property 'MW' does not exist in type" and
  `Timestamp` is a GQL reserved word. The graph model only materializes non-timeseries
  properties and edges; time series are meant to be queried through a separate surface
  (KQL/Eventhouse, entity Overview widgets, or the Data Agent's NL2Ontology routing,
  which splits a question into GQL for structure + KQL for observations). So GQL alone
  can never aggregate a measure that lives in a time series — a graph-side MWh answer
  requires either an aggregate entity (the UnitDay pattern) or cross-engine routing.
  Re-verified 2026-08-18 on the current service with a throwaway `ts_probe` ontology:
  a **lakehouse** Delta table IS an accepted TimeSeries source (the docs say "OneLake or
  an eventhouse" — RTI is not required). The binding deploys, `getDefinition` echoes it
  back as `TimeSeries`/`SETTLEMENTDATE`, the graph refresh Completes and static
  properties query fine — but `u.InitialMW` still fails with 42000 "Property 'InitialMW'
  does not exist in type (:TsUnit)". The timestamp column must be a real
  timestamp/datetime (TIMESTAMPTZ → delta `timestamp` works; the split DateKey+TimeHHMM
  pair cannot be selected), and per the type map a delta `timestamp_ntz` binds as String.
- **A leaf-grain fact table CAN live in the graph** (the v4 result, still the basis for
  `Observation` in `ontology.py`)**:**
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
- **v5: the fix for bad measure answers was a smaller entity, not a better prompt.**
  `ontology.py` adds two tiny entity sets on the new marts — **RegionInterval** (373k
  nodes from `fct_region`: demand, net interchange, available generation, and the ONE
  authoritative regional price) and **Flow** (298k from `fct_interconnector_derived`) —
  alongside v4's 11.9M `Observation`. Together they add ~5% to the load and remove the
  entire class of question v4 got wrong. The proof: *"average spot price in SA1 on
  2026-08-09"* returned **$111.12 from a 1,329,227-row fan-out** under v4 and **$35.66 over
  288 intervals** under v5, first try.
- **v4's "ground truth" price was the right answer to the wrong question.** Averaging
  `Price` over `Observation` gives **35.145** — a *unit-weighted* mean over 15,032 unit-rows,
  because the regional price is copied onto every unit. The true time-average over 288
  intervals is **35.665**. There was no way to compute the correct number from `fct_summary`
  alone, which is why v4's instructions had to tell the agent to caveat it. `fct_region`
  makes it a plain `avg()`.
- **Rename around GQL reserved words instead of documenting them.** v5 renames `Unit` →
  `GeneratingUnit` and `Interval` → `TimeHHMM`. Nothing in v5 needs a backtick, so the whole
  "a bare `(u:Unit)` is a *syntax* error" trap disappears rather than being something the
  agent must remember. `TimeHHMM` also stops the name from lying — the column is HHMM.
- **Tell the agent which entity to use before telling it how to query.** The single biggest
  accuracy lever in `data_agent.py`'s instructions is now a routing rule near the top:
  region-wide questions → `RegionInterval`, unit/station/company → `Observation`,
  between-regions → `Flow`, structure-only → the dimensions. Wrong entity, not wrong syntax,
  was what produced the confidently wrong numbers.
- **RELATIVE dates are unreliable; EXPLICIT dates are exact. This one is not fixable by
  prompting.** Ask *"average demand in QLD1 between 2026-08-05 and 2026-08-11"* and you get
  5915.88 MW over **1,777 intervals** — exact. Ask *"last week"* and you often get 6012.53 MW
  over **78,146 intervals**, which is the entire 8-year history for that region
  (390,730 RegionInterval rows ÷ 5 regions = 78,146 exactly). The agent anchors the window
  correctly, states the right date range in prose, and then emits an aggregate with the
  `DateKey` predicate missing. Raw GQL with the same filter is correct, so it is the
  NL2Ontology planner losing the predicate across its two-step plan, not the engine.
  Mitigations tried, in order: a forceful "anchoring is only half the job" rule; an interval
  -count sanity table; and then **baking the latest date into `aiInstructions` at deploy
  time** — `data_agent.py` queried `max(date) FROM mart.fct_region` and substituted
  `__LATEST_DATE__`/`__WEEK_START__`/`__YESTERDAY__`, paired with a rule telling the agent to
  NEVER probe. **That third one has been REMOVED — don't reintroduce it.** It did help (SA1
  spare capacity came back exact at 2,042 MW over 1,777 intervals) but it was wrong in a way
  the numbers hid: a baked constant is only true until the next dbt run, the pipeline lands
  new data **every 720m** while the agent is redeployed only by a manual `deploy=full`, and
  the paired "never probe" rule forbade the agent from ever noticing. It drifted two days
  within hours. A confidently-stated wrong date is a worse failure than a dropped predicate,
  because nothing surfaces it.
  What replaced it is a rule instead of a constant: **"today"/"latest" means the latest
  `DateKey` in the data**, which is true forever and needs no redeploy. The agent probes for
  the anchor (`MATCH (ri:RegionInterval) RETURN max(ri.DateKey)`) and subtracts itself — GQL
  has no date arithmetic, so nothing else can. The two-step plan is still the weak spot, so
  the instructions now counter it where it actually breaks: the literal dates must appear in
  the **aggregate's own** `WHERE` clause, not just in the anchor query.
  QLD1 demand still fails intermittently on this question shape. **Treat it as
  non-determinism and use explicit dates for anything that matters.**
  What DOES work reliably is making the failure visible: because the instructions force a row
  count alongside every measure, a wrong answer announces itself as "78,146 intervals" instead
  of arriving as a plausible number. Keep that rule.
  Side effect worth knowing: with the `max(date)` query gone, `data_agent.py` no longer opens
  a lakehouse at all — no `import duckrun`, no `LAKEHOUSE` constant, pure Fabric REST.
- **Ontology, GraphModel and DataAgent do NOT appear in `GET /workspaces/{ws}/items`.**
  The unfiltered list omits them entirely; `items?type=Ontology` does return them, as do
  their own `/ontologies`, `/GraphModels`, `/dataAgents` collections. Don't conclude an
  ontology is missing because the item list looks short.
- **`folderId` on create works for the ontology and the data agent; the graph model
  inherits.** Both create calls accept `"folderId"` in the body (and `POST
  /items/{id}/move` with `{"targetFolderId": …}` fixes an item created earlier in the wrong
  place). The graph model is a **child** of the ontology — moving it alone fails with
  `CannotMoveChildOnly: "The child item cannot be moved without its parent item"` — so
  placing the ontology places the graph automatically. Verified: all three land in `aemo`
  with only the ontology and agent asking for it.
- **Changed data needs an explicit graph refresh; only schema changes auto-refresh.**
  `POST /v1/workspaces/{ws}/items/{graphId}/jobs/instances?jobType=RefreshGraph`
  (the job type is undocumented — `Refresh`/`GraphRefresh` return `InvalidJobType`).
  `ontology.py` fires it on every run; it takes a couple of minutes to land. Since the
  Operations Agent work, **every data load fires it too**: CI Phase 3.5 runs
  `refresh_graph.py` and the `run` notebook's last cell inlines the same call (only the dbt
  project lives on the lakehouse, so the notebook can't import the script). Both are
  deliberately tolerant — no graph deployed, or a refresh already in flight, print and
  carry on — because a graph hiccup must never fail a data load. Double-firing on a
  `deploy=full` run (ontology.py refreshes too) is harmless for the same reason.
- **Interconnector is a graph NODE, so a region-to-region hop is two edges** — and GQL
  rejects a quantified pattern spanning it ("Parenthesized path pattern expressions must
  be formed of exactly one edge pattern in between two node patterns"). `shutdown.py`
  therefore walks one hop at a time and closes in Python. Modelling the link as an edge
  would allow `{1,n}` but lose per-link filtering, because relationship instances carry
  no properties. `shutdown.py` walks the closure in Python for exactly this reason.
- **GQL date ranges work via a YYYYMMDD INTEGER property.** GQL has **no date functions or
  literals**, so every fact mart carries `DateKey`
  (`CAST(strftime(date, '%Y%m%d') AS INT)`). It compares and orders numerically, so
  `WHERE ri.DateKey >= 20260805 AND ri.DateKey <= 20260811` filters a week fine — unquoted,
  no per-day key-equality OR chains. Dates are banned as entity **key** parts (String or
  Integer only), which is why `DateKey` exists at all.
  It was an ISO string (`CAST(date AS VARCHAR)`) until it was pointed out that Integer is
  just as valid a key part — `TimeHHMM` had been proving that all along. The integer is
  smaller, needs no quoting in generated GQL, and sorts identically.
  **The one trap: subtract CALENDAR days, not integers.** The day before `20260801` is
  `20260731`, not `20260800`. `data_agent.py`'s instructions say so explicitly, because the
  agent does this arithmetic itself.
  `date` stays a real `DATE` alongside it — that is what Power BI's calendar relationship
  joins on, and a `DATE` is strictly better there than an integer.
- **The latest day is almost always PARTIAL, so never divide by 288.** `fct_summary` carries
  an intraday tail, so `max(DateKey)` is today with only the intervals published so far —
  49 of 288 at the time of writing, which is why a week comes to 1,777 rows and not 2,016.
  Averaging a region's demand as `sum(TotalDemand) / 288` therefore under-reports by the
  fraction of the day elapsed (it showed TAS1 at 175 MW instead of ~1,030). Use `avg()` and
  add the per-region averages, and print `count()` alongside so a partial day is visible
  rather than silently wrong — the same discipline the data agent's instructions enforce.
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

## Operational ontology — the Operations Agent (`operations_agent.py`, gated on rollout)

The operational answer to "the analytical ontology adds little": a **Fabric Operations
Agent** grounded on `aemo_nem`. It compiles NL instructions into a playbook of rules,
evaluates each rule's graph query every ~5 minutes, Teams-messages the creator when one
fires, and carries a `FabricJobAction` that runs `run_pipeline` on approval from the Teams
card. `operations_agent.py` deploys it code-first (single `Configurations.json` part, the
documented OperationsAgentV1 format), mirroring `data_agent.py`'s helpers.

**The whole stack now co-locates with the agent (plan B): everything deploys to the
parameterized workspace — default `sqlengines` (450bf196), West Europe, `FabricIQ` folder —
because the old workspace's P1 sits in East US, one of exactly two US regions where
"Operations agent (preview)" is excluded** (the other is South Central US, which also lacks
Ontology); see https://learn.microsoft.com/fabric/admin/region-availability. Creating the
agent there answers `403 FeatureNotAvailable`. Tenant config was eliminated first and is
NOT the problem (`EnableAOAI`, both cross-geo AOAI switches, `OntologyPreview` all on; not
a trial; no disabled tenant setting mentions the item type). The agent is
`aemo_nem_ops` (a59fd3b4-b910-40cb-8102-7030255be8fd), same workspace as the ontology.
A **cross-workspace ontology dataSource also works** (each `dataSources` entry carries its
own `workspaceId`) — that ran for a while before plan B; co-location just removes the
unproven cross-REGION agent→graph hop.

Everything below was measured against the live service on 2026-08-18, and the live service
disagrees with the docs article on several points:

- **USER identities only** — the API rejects service principals, so the script is NOT in
  `deploy.py` (CI's OIDC identity cannot call it). Run `python operations_agent.py` from a
  laptop under `az login`.
- **The definition is ONE `Configurations.json` part** carrying a `$schema` field, with
  **NO `playbook` key** — the article calls playbook required, but the service's own
  skeleton omits it. (`OperationsAgentV1.json`, the part name in the REST API sample,
  fails with "Missing artifact content (BlobId)" — i.e. wrong name.)
- **A `FabricJobAction` ALWAYS fails through the definition path** — 400 `UnknownError`
  for every schema-valid variant (same- and cross-workspace, Pipeline and RunNotebook,
  with/without `parameters`), while a `PowerAutomateAction` in the same slot returns 200.
  Some registration the portal does isn't done by definition import. So the agent ships
  **Teams-alert-only** (the default DM to the creator needs no action config);
  `INCLUDE_JOB_ACTION` in the script preserves the run-the-pipeline action for when the
  path is fixed. An action added in the PORTAL would be **wiped** by the next
  `updateDefinition` — `--dump` and merge first.
- **`getDefinition` scrubs GUIDs on read**: the stored dataSource id and `.platform`
  `logicalId` echo back as `00000000-…`. The write DID validate and store the real id —
  pushing the zeros back fails with 404 `EntityNotFound`, proving ids resolve on write.
  A dump is therefore NOT round-trippable; always deploy from the script.
- **`shouldRun: true` is coerced to false on import.** Starting needs the playbook, and
  generating it is portal-only: open the agent → **Generate playbook** → review → **Start**
  (repeat after instruction changes). That is the ONE manual step; everything else is code.
- Bare create (`POST /operationsAgents` with just displayName) → 201; then
  `updateDefinition`. `PATCH /operationsAgents/{id}` fixes displayName/description.
  **Create WITH a definition in one call also works when `actions` is empty** (measured:
  the plan-B agent was created that way) — the earlier one-call failure was purely the
  FabricJobAction.

The *other* operational route — **Rules on ontology entity types (Activator-backed)** —
was considered and dropped: it requires at least one **TimeSeries-bound property** (our v5
ontology has none, by design — see the v3 dead end) and rules are authored portal-only.

`RegionInterval` carries **`LorSurplus`** (< 0 = Lack Of Reserve breach) and
**`MarketSuspendedFlag`** (≠ 0 = market suspended) since this work — the two genuine
operational signals, already `DOUBLE` in `fct_region`. The additive edit's `--check`
signature was 2 CHANGED (RegionInterval definition + data binding), 0 added, 0 removed.

Still unverified (needs the portal start + the "Fabric Operations Agent" Teams app): the
playbook compiling sensible rules from the instructions, and alerts actually arriving.

## Running a deployed item on Fabric

`ws.run("run")` runs a deployed notebook (or pipeline) and waits for it — no `-i '{}'`
incantation. `deploy.py` deliberately does NOT run the notebook: CI's own dbt run already
wrote the Delta tables to the same lakehouse, so it would be redundant. The notebook is
exercised by the scheduled pipeline.

## The ontology model is `ontology.yaml`; `ontology.py` only deploys it

Entities, properties, bindings and relationships are **data** and live in `ontology.yaml`.
`ontology.py` loads it, hashes the parts, and pushes them — nothing about the ontology's shape
is decided in the Python any more. Adding an entity is a YAML edit.

- **Every id is derived from a NAME by hashing**, so a rename is not a rename. `big_id("entity",
  name)` / `big_id("prop", entity, prop)` / `big_id("rel", name)` mean a renamed thing mints a
  new type and **orphans the old one** — the deploy adds rather than updates, and the stale type
  has to be deleted in the portal. Changing a `table:` or a source column is safe; changing a
  name on the left-hand side is not.
- **`python ontology.py --check`** builds the parts and diffs them against whatever
  `download.py` last fetched into `fabric_download/aemo_nem.Ontology/`, then exits — no token,
  no deploy, exit 1 if anything differs. This is how you confirm a YAML edit does what you meant
  *before* shipping it. An `ADDED`+`REMOVED` pair with no `CHANGED` is the signature of an
  accidental rename (verified: renaming `Flow` → `PowerFlow` reports 2 added, 2 removed, 0
  changed).
- **`--check` compares only the keys we SEND, not equality.** Fabric enriches what it stores:
  every part gains a `$schema`, `.platform` gains a `config`/`logicalId`, and entity definitions
  gain `untypedProperties`. A plain `==` reports all 40 parts as changed on an untouched model.
  `sends_same()` walks our side only and ignores extra keys.
- Properties are `Name: [sourceColumn, valueType]` — a 2-list unpacks exactly like the tuples it
  replaced, so the parts-assembly code was unchanged by the move.
- **`pyyaml` is named explicitly in the workflow's pip install.** It already arrived via
  `duckrun → dbt-core`, but a transitive dependency shouldn't be load-bearing.
- The environment (workspace, lakehouse, schema, folder) stays as constants in `ontology.py`;
  only the model moved. The ontology's own `name` and `description` come from the YAML.

## Reading an item back out of Fabric — `download.py`

`python download.py` writes a deployed item's definition to the **gitignored** `fabric_download/`
as `{displayName}.{ItemType}/`, decoded, in the git-integration part layout. No args = the
ontology and the data agent; `python download.py <collection> [displayName]` takes any collection
(`ontologies`, `dataAgents`, `GraphModels`, `notebooks`, …). It is a **laptop inspection tool**:
`deploy.py` does not call it, CI does not run it, and it never opens a lakehouse.

- **It is not a round trip.** `ontology.py` and `data_agent.py` build their parts from Python
  (`ENTITIES`/`RELATIONSHIPS`, `INSTRUCTIONS`) and never read the filesystem — they stay the
  source of truth. The download exists to see what Fabric *actually stored* — the only way to
  check the undocumented `"type": "ontology"` data-source bet (verified: the stored
  `datasource.json` echoes it straight back), and to read the live `aiInstructions` rather
  than the ones you think you deployed. That second use is what killed the baked-date hack:
  downloading showed the deployed agent asserting a date **one day stale**, which is how the
  drift stopped being theoretical.
- **`OUT` must not be pointed at `fabric_items/`.** duckrun's folder deploy raises
  `unsupported item type` on anything outside VariableLibrary / Notebook / SemanticModel /
  DataPipeline, so downloading straight into it breaks the whole deploy.
- **Don't commit the downloaded parts.** The ontology expands to **40 files / 34 KB** whose
  directories are 19-digit BigInts minted by `big_id()` and whose bindings are GUIDs, so a diff
  reads `EntityTypes/8349478236053681467/definition.json changed`. `ontology.py`'s
  `ENTITIES`/`RELATIONSHIPS` dicts are the readable source those 40 files are compiled from —
  keeping both would be checking in build output next to its generator.
- **`getDefinition` returns `.platform` even when the item's documented parts table omits it.**
  The Data Agent parts table lists no `.platform` — and `data_agent.py` sends none, which is why
  it must omit `?updateMetadata=true` — yet the download comes back with one (Fabric's "Get Item
  definition always returns the platform file"). Its presence in `fabric_download/` is not a sign
  the script sends it.
- **`getDefinition` may answer 200 OR 202.** On 202 the payload is at `{Location}/result`, not in
  the operation-status body — so the shared `call()` helper is not enough, and `download.py` has
  its own `get_definition()` handling both (`data_agent.py --dump` reimplements the 202 half).
- **`GraphModels` supports `getDefinition` too**, though it is undocumented: 6 parts
  (`graphDefinition.json`, `graphType.json`, `graphSettings.json`, `dataSources.json`,
  `stylingConfiguration.json`, `.platform`).
- Sanity check for the ontology: **8 entity types, 11 relationship types, 40 parts** — matching
  `ENTITIES`/`RELATIONSHIPS` and the count `ontology.py` prints on deploy.
