"""Benchmark two semantic models by running the SAME heavy DAX queries against each
over the XMLA endpoint and timing them — to see whether the `_vorder` model (which
reads a Fabric V-Order copy of the fact) is faster in Direct Lake than the delta-rs base.

NOT a correctness check — both models read the same data, so the numbers are identical
by construction. What differs is the Delta layout, which changes how much the Direct Lake
engine has to transcode (cold) and scan (hot). We measure both as query wall-clock.

COLD is forced per query by DEHYDRATING the model first: a TMSL `clearValues` refresh evicts
all transcoded column data from memory, then a `full` refresh reframes (on Direct Lake that's
metadata only — no transcode), so the next query pays the full cold Delta→memory cost. We
dehydrate before EACH query because the queries share the big fact columns (mw/price/DUID/
date/time) — without a per-query dehydrate only the first query would be cold.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless (GitHub Actions, windows-latest) — see .github/workflows/xmla_compare.yml.

Env in:
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — AAD access token for https://analysis.windows.net/powerbi/api
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — repetitions per query per model (default 3); best (min) is reported
  BENCH_COLD     — "true"/"false": measure cold via dehydrate (default true). Falls back to
                   hot-only automatically if the token can't run the refresh (needs write).

Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import json
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


def dehydrate_model(conn, model):
    """Evict all column data (clearValues) then reframe (full = metadata only on Direct Lake),
    leaving the model cold — the next query pays the full Delta->memory transcode cost."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    for refresh_type in ("clearValues", "full"):
        tmsl = json.dumps({"refresh": {"type": refresh_type, "objects": [{"database": model}]}})
        AdomdCommand(tmsl, conn).ExecuteNonQuery()


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


def bench_model(workspace, model, token, runs, want_cold):
    print(f"\n=== Benchmarking {model} (runs={runs}, cold={want_cold}) ===")
    conn = open_conn(workspace, model, token)
    can_cold = want_cold
    if want_cold:
        try:
            dehydrate_model(conn, model)
            print("  dehydrate: OK (per-query cold timing enabled)")
        except Exception as e:
            can_cold = False
            print(f"  dehydrate: unavailable ({str(e).splitlines()[0][:120]}) — hot timing only")
    results = {}
    try:
        for name, dax in QUERIES:
            cold, hot, rowcount = [], [], None
            for _ in range(runs):
                if can_cold:
                    dehydrate_model(conn, model)          # cold state
                    c, rows = run_query(conn, dax)         # first hit = cold transcode
                    cold.append(c)
                    rowcount = rows
                h, rows = run_query(conn, dax)             # resident = hot
                hot.append(h)
                rowcount = rows
            res = {"hot_min": min(hot), "hot_median": statistics.median(hot), "rows": rowcount}
            if cold:
                res["cold_min"] = min(cold)
                res["cold_median"] = statistics.median(cold)
            results[name] = res
            cold_s = f"cold_min={res['cold_min']:8.1f}ms  " if cold else ""
            print(f"  {name:<28} {cold_s}hot_min={res['hot_min']:8.1f}ms  rows={rowcount}")
    finally:
        conn.Close()
    return results, can_cold


def discover_models():
    root = Path(__file__).parent
    names = sorted(p.name.removesuffix(".SemanticModel")
                   for p in (root / "fabric_items").glob("*.SemanticModel"))
    if len(names) < 2:
        sys.exit(f"Need at least 2 semantic models to benchmark, found {len(names)}: {names}")
    base = min(names, key=len)
    return base, [n for n in names if n != base]


def _write_summary(text):
    """Append markdown to the GitHub Actions job summary (renders as a real table)."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")


def _render_console(title, headers, rows, aligns, sep_before_last=False):
    """A boxed, aligned unicode table to stdout."""
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(str(c)))
    line = lambda l, m, rt: l + m.join("─" * (w + 2) for w in widths) + rt
    def frow(cells):
        parts = []
        for i, c in enumerate(cells):
            c = str(c)
            parts.append(" " + (c.rjust(widths[i]) if aligns[i] == "r" else c.ljust(widths[i])) + " ")
        return "│" + "│".join(parts) + "│"
    print(f"\n{title}")
    print(line("┌", "┬", "┐"))
    print(frow(headers))
    print(line("├", "┼", "┤"))
    body = rows[:-1] if sep_before_last else rows
    for r in body:
        print(frow(r))
    if sep_before_last:
        print(line("├", "┼", "┤"))
        print(frow(rows[-1]))
    print(line("└", "┴", "┘"))


def compare_table(title, base, model, base_res, opt_res, key):
    base_tot = opt_tot = 0.0
    wins = 0
    rows = []
    for name, _ in QUERIES:
        b = base_res[name][key]
        o = opt_res[name][key]
        base_tot += b
        opt_tot += o
        speedup = (b / o) if o else float("inf")
        wins += 1 if o < b else 0
        rows.append((name, b, o, speedup, "opt" if o < b else ("base" if o > b else "tie")))
    overall = (base_tot / opt_tot) if opt_tot else float("inf")
    total_w = "opt" if opt_tot < base_tot else ("base" if opt_tot > base_tot else "tie")
    mshort = model.rsplit("_", 1)[-1]  # e.g. aemo_electricity_vorder -> "vorder"
    winner_lbl = {"opt": model, "base": base, "tie": "tie"}
    factor = overall if overall >= 1 else (1.0 / overall if overall else 0.0)
    headline = (f"{winner_lbl[total_w]} is {factor:.2f}× faster overall"
                f" — {mshort} wins {wins}/{len(QUERIES)}")

    # ---- boxed console table ----
    mark = {"opt": f"{mshort} ✔", "base": "base ✔", "tie": "tie"}
    disp = [(n, f"{b:,.1f}", f"{o:,.1f}", f"{s:.2f}×", mark[w]) for (n, b, o, s, w) in rows]
    disp.append(("TOTAL", f"{base_tot:,.1f}", f"{opt_tot:,.1f}", f"{overall:.2f}×", mark[total_w]))
    _render_console(title, ("query", f"{base} (ms)", f"{model} (ms)", f"base/{mshort}", "winner"),
                    disp, ("l", "r", "r", "r", "l"), sep_before_last=True)
    print(f"  → {headline}")

    # ---- markdown for the job summary ----
    emoji = {"opt": f"🟢 {mshort}", "base": "🔴 base", "tie": "⚪ tie"}
    md = [f"### {title}", "",
          f"| Query | {base} (ms) | {model} (ms) | base / {mshort} | Winner |",
          "|:--|--:|--:|--:|:--|"]
    for (n, b, o, s, w) in rows:
        md.append(f"| `{n}` | {b:,.1f} | {o:,.1f} | {s:.2f}× | {emoji[w]} |")
    md.append(f"| **TOTAL** | **{base_tot:,.1f}** | **{opt_tot:,.1f}** | "
              f"**{overall:.2f}×** | **{emoji[total_w]}** |")
    md += ["", f"**{headline}.**", ""]
    _write_summary("\n".join(md))


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    token = os.environ["PBI_TOKEN"].strip()
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    runs = int(os.environ.get("BENCH_RUNS", "3"))
    want_cold = (os.environ.get("BENCH_COLD", "true").strip().lower() != "false")
    gap = int(os.environ.get("BENCH_GAP_SECONDS", "300"))  # idle gap between models (>CU smoothing)

    _load_adomd(adomd_dir)
    base, others = discover_models()
    print(f"Workspace : {workspace}")
    print(f"Base model: {base}")
    print(f"Compare   : {', '.join(others)}")

    _write_summary(
        f"# 🔍 XMLA benchmark — `{', '.join(others)}` vs `{base}`\n\n"
        f"Workspace `{workspace}` · min of **{runs}** runs per query · "
        f"same data (numbers identical) — only speed differs. "
        f"Lower ms is better; **base/model ratio > 1× means the compared model is faster**.\n")

    base_res, base_cold = bench_model(workspace, base, token, runs, want_cold)

    for model in others:
        if gap:
            print(f"\n⏳ Idle gap: sleeping {gap}s before {model} so the Fabric capacity chart "
                  f"shows a clean base → gap → {model} separation...", flush=True)
            time.sleep(gap)
        opt_res, opt_cold = bench_model(workspace, model, token, runs, want_cold)
        if base_cold and opt_cold:
            compare_table(f"{model} vs {base}  —  COLD (min of {runs}, dehydrated per query)",
                          base, model, base_res, opt_res, "cold_min")
        compare_table(f"{model} vs {base}  —  HOT (min of {runs})",
                      base, model, base_res, opt_res, "hot_min")


if __name__ == "__main__":
    main()
