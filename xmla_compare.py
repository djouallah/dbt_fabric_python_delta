"""Benchmark two semantic models by running the SAME heavy DAX queries against each
over the XMLA endpoint and timing them — to see whether the `_optimized` model (which
reads a `sorted by auto` clustered copy of the fact) is faster in Direct Lake.

NOT a correctness check — both models read the same data, so the numbers are identical
by construction. What differs is the Delta layout, which changes how much the Direct Lake
engine scans. We measure that as query wall-clock time.

Cold is not measurable here: Power BI ClearCache does NOT evict Direct Lake's transcoded
column data, and the only true cold state is a freshly (re)created model. So instead we
compare HOT: run a few warm-up passes to transcode/cache the columns, discard them, then
time the hot runs. Both models get the same warm-up → apples to apples.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless (GitHub Actions, windows-latest) — see .github/workflows/xmla_compare.yml.

Env in:
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — AAD access token for https://analysis.windows.net/powerbi/api
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_WARMUP   — discarded warm-up passes over the suite per model (default 2)
  BENCH_RUNS     — measured hot repetitions per query per model (default 5)

Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import os
import statistics
import sys
import time
from pathlib import Path

# Windows CI console defaults to cp1252, which can't encode the emoji/glyphs — force UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Heavy queries: each forces a large scan of the ~140M-row fact but returns a SMALL result,
# so we time the engine (scan/aggregate), not row transfer over the wire. Measures/columns
# referenced all exist in model.bim (fct_summary, dim_duid, dim_calendar + the model measures).
QUERIES = [
    ("region_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MWh", [Total MWh], "AvgP", [Avg Price], "Gens", [Generator Count])'),
    ("fuel_x_region",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[FuelSourceDescriptor], dim_duid[Region], '
     '"MWh", [Total MWh], "MW", [Total MW])'),
    ("timeofday_x_region",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[time], dim_duid[Region], '
     '"MWh", [Total MWh], "AvgP", [Avg Price])'),
    ("duid_x_month",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], dim_calendar[month], '
     '"MWh", [Total MWh])'),
    ("filtered_nsw_2024_by_duid",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_summary[DUID], "MWh", [Total MWh], "AvgP", [Avg Price]), '
     'dim_duid[Region] = "NSW1", dim_calendar[year] = 2024)'),
    ("scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"RevenueProxy", SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     '"DistinctDUID", DISTINCTCOUNT(fct_summary[DUID]), '
     '"Rows", COUNTROWS(fct_summary))'),
    ("topn_duid_by_mwh",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], '
     '"MWh", [Total MWh]), [MWh], DESC)'),
]


def _load_adomd(adomd_dir: str):
    """Make Microsoft.AnalysisServices.AdomdClient importable via pythonnet."""
    import clr  # pythonnet
    hits = glob.glob(os.path.join(adomd_dir, "**", "Microsoft.AnalysisServices.AdomdClient.dll"),
                     recursive=True)
    if not hits:
        sys.exit(f"ADOMD client DLL not found under {adomd_dir!r}")
    hits.sort(key=lambda p: ("netcore" not in p.lower() and "net6" not in p.lower(), len(p)))
    d = os.path.dirname(hits[0])
    if d not in sys.path:
        sys.path.append(d)
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    print(f"Loaded ADOMD from {hits[0]}")


def open_conn(workspace: str, model: str, token: str):
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    conn_str = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
        f"Initial Catalog={model};User ID=;Password={token};"
    )
    conn = AdomdConnection(conn_str)
    conn.Open()
    return conn


def run_query(conn, dax: str):
    """Execute dax, drain all rows, return (elapsed_ms, row_count)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    t0 = time.perf_counter()
    reader = AdomdCommand(dax, conn).ExecuteReader()
    rows = 0
    try:
        fc = reader.FieldCount
        while reader.Read():
            for i in range(fc):
                reader.GetValue(i)
            rows += 1
    finally:
        reader.Close()
    return (time.perf_counter() - t0) * 1000.0, rows


def bench_model(workspace, model, token, warmup, runs):
    print(f"\n=== Benchmarking {model} (warmup={warmup} discarded, hot runs={runs}) ===")
    conn = open_conn(workspace, model, token)
    results = {}
    try:
        # Warm-up: transcode/cache the columns the suite touches. Timings discarded.
        for w in range(warmup):
            for _, dax in QUERIES:
                run_query(conn, dax)
            print(f"  warm-up pass {w + 1}/{warmup} done")
        # Measured HOT runs.
        for name, dax in QUERIES:
            times, rowcount = [], None
            for _ in range(runs):
                ms, rows = run_query(conn, dax)
                times.append(ms)
                rowcount = rows
            results[name] = {"min": min(times), "median": statistics.median(times), "rows": rowcount}
            print(f"  {name:<28} min={results[name]['min']:8.1f}ms  "
                  f"median={results[name]['median']:8.1f}ms  rows={rowcount}")
    finally:
        conn.Close()
    return results


def discover_models():
    root = Path(__file__).parent
    names = sorted(p.name.removesuffix(".SemanticModel")
                   for p in (root / "fabric_items").glob("*.SemanticModel"))
    if len(names) < 2:
        sys.exit(f"Need at least 2 semantic models to benchmark, found {len(names)}: {names}")
    base = min(names, key=len)
    return base, [n for n in names if n != base]


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    token = os.environ["PBI_TOKEN"].strip()
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    warmup = int(os.environ.get("BENCH_WARMUP", "2"))
    runs = int(os.environ.get("BENCH_RUNS", "5"))

    _load_adomd(adomd_dir)
    base, others = discover_models()
    print(f"Workspace : {workspace}")
    print(f"Base model: {base}")
    print(f"Compare   : {', '.join(others)}")

    base_res = bench_model(workspace, base, token, warmup, runs)

    for model in others:
        opt_res = bench_model(workspace, model, token, warmup, runs)
        print(f"\n============ {model}  vs  {base}   (HOT, best of {runs} after {warmup} warm-ups) ============")
        header = f"{'query':<28} {'base(ms)':>12} {'opt(ms)':>12} {'speedup':>9}"
        print(header)
        print("-" * len(header))
        base_tot = opt_tot = 0.0
        wins = 0
        for name, _ in QUERIES:
            b = base_res[name]["min"]
            o = opt_res[name]["min"]
            base_tot += b
            opt_tot += o
            speedup = (b / o) if o else float("inf")
            wins += 1 if o < b else 0
            flag = "faster" if o < b else ("slower" if o > b else "equal")
            print(f"{name:<28} {b:12.1f} {o:12.1f} {speedup:8.2f}x {flag}")
        print("-" * len(header))
        overall = (base_tot / opt_tot) if opt_tot else float("inf")
        print(f"{'TOTAL':<28} {base_tot:12.1f} {opt_tot:12.1f} {overall:8.2f}x")
        print(f"\n{model}: faster on {wins}/{len(QUERIES)} queries; "
              f"overall {overall:.2f}x vs {base} (hot timing, min of {runs}).")


if __name__ == "__main__":
    main()
