"""Query every semantic model in fabric_items/ with the SAME DAX over the XMLA
endpoint and diff the results against the base model.

Purpose: prove that a `_optimized` model (which reads a re-clustered copy of the
fact) returns identical numbers to the base model. Uses the XMLA endpoint (ADOMD.NET),
NOT the /executeQueries REST endpoint — the REST DAX endpoint is throttled hard.

Run headless (GitHub Actions, windows-latest) — see .github/workflows/xmla_compare.yml.
Auth: a Power BI AAD access token passed as the XMLA connection Password.

Env in:
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — AAD access token for https://analysis.windows.net/powerbi/api
  PBI_DAX        — optional DAX; blank -> DEFAULT_DAX below
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
                   (the NuGet install dir; we glob for the dll under it)

Exit code 0 = all models match the base, 1 = a difference (or an error).
"""
import glob
import os
import sys
from pathlib import Path

# Windows CI console defaults to cp1252, which can't encode the ✅/❌ status glyphs — force UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# A query that exercises the fact aggregations at Region grain, so ANY data
# difference between the base and optimized tables surfaces. Only measures/columns
# that exist in the model are referenced (see model.bim).
DEFAULT_DAX = """
EVALUATE
SUMMARIZECOLUMNS(
    dim_duid[Region],
    "Total MWh", [Total MWh],
    "Avg Price", [Avg Price],
    "Generator Count", [Generator Count],
    "Rows", COUNTROWS(fct_summary)
)
ORDER BY dim_duid[Region]
""".strip()

# Round floats before comparing so identical data can't fail on a last-bit wobble.
FLOAT_TOL_DECIMALS = 6


def _load_adomd(adomd_dir: str):
    """Make Microsoft.AnalysisServices.AdomdClient importable via pythonnet."""
    import clr  # pythonnet
    dll = None
    for pat in ("**/Microsoft.AnalysisServices.AdomdClient.dll",):
        hits = glob.glob(os.path.join(adomd_dir, pat), recursive=True)
        if hits:
            # Prefer a netcore build (matches PYTHONNET_RUNTIME=coreclr); else take any.
            hits.sort(key=lambda p: ("netcore" not in p.lower() and "net6" not in p.lower(),
                                     len(p)))
            dll = hits[0]
            break
    if not dll:
        sys.exit(f"ADOMD client DLL not found under {adomd_dir!r}")
    d = os.path.dirname(dll)
    if d not in sys.path:
        sys.path.append(d)
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    print(f"Loaded ADOMD from {dll}")


def _normalize(v):
    if isinstance(v, float):
        return round(v, FLOAT_TOL_DECIMALS)
    # ADOMD may hand back System.Decimal / DBNull etc — str() is stable for comparison
    if v is None:
        return None
    return v


def query(workspace: str, model: str, token: str, dax: str):
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand
    conn_str = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
        f"Initial Catalog={model};User ID=;Password={token};"
    )
    conn = AdomdConnection(conn_str)
    conn.Open()
    try:
        reader = AdomdCommand(dax, conn).ExecuteReader()
        try:
            cols = [reader.GetName(i) for i in range(reader.FieldCount)]
            rows = []
            while reader.Read():
                rows.append(tuple(_normalize(reader.GetValue(i)) for i in range(reader.FieldCount)))
        finally:
            reader.Close()
        return cols, rows
    finally:
        conn.Close()


def discover_models():
    """(base_model, [other_models]) from fabric_items/*.SemanticModel.

    Base = the shortest name (e.g. 'aemo_electricity'); the rest (e.g.
    'aemo_electricity_optimized') are compared against it."""
    root = Path(__file__).parent
    names = sorted(p.name.removesuffix(".SemanticModel")
                   for p in (root / "fabric_items").glob("*.SemanticModel"))
    if len(names) < 2:
        sys.exit(f"Need at least 2 semantic models to compare, found {len(names)}: {names}")
    base = min(names, key=len)
    others = [n for n in names if n != base]
    return base, others


def _print_rows(cols, rows, limit=50):
    print("  " + " | ".join(str(c) for c in cols))
    for r in rows[:limit]:
        print("  " + " | ".join(str(x) for x in r))
    if len(rows) > limit:
        print(f"  … ({len(rows) - limit} more rows)")


def diff(base_res, other_res):
    """Return a list of human-readable difference lines (empty = identical)."""
    (bcols, brows), (ocols, orows) = base_res, other_res
    diffs = []
    if bcols != ocols:
        diffs.append(f"columns differ: base={bcols} other={ocols}")
        return diffs
    if len(brows) != len(orows):
        diffs.append(f"row count differs: base={len(brows)} other={len(orows)}")
    for i, (br, orow) in enumerate(zip(brows, orows)):
        if br != orow:
            diffs.append(f"row {i} differs:\n    base : {br}\n    other: {orow}")
    return diffs


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    token = os.environ["PBI_TOKEN"].strip()
    dax = (os.environ.get("PBI_DAX") or "").strip() or DEFAULT_DAX
    adomd_dir = os.environ.get("ADOMD_DIR", ".")

    _load_adomd(adomd_dir)

    base, others = discover_models()
    print(f"Workspace : {workspace}")
    print(f"Base model: {base}")
    print(f"Compare   : {', '.join(others)}")
    print("DAX:\n" + "\n".join("  " + l for l in dax.splitlines()))
    print("-" * 72)

    base_res = query(workspace, base, token, dax)
    print(f"[{base}] {len(base_res[1])} rows")
    _print_rows(*base_res)
    print("-" * 72)

    failed = False
    for model in others:
        other_res = query(workspace, model, token, dax)
        d = diff(base_res, other_res)
        if d:
            failed = True
            print(f"❌ {model} DIFFERS from {base}:")
            for line in d:
                print("  " + line)
        else:
            print(f"✅ {model} matches {base} ({len(other_res[1])} rows identical)")
        print("-" * 72)

    if failed:
        print("::error::XMLA comparison found differences between semantic models")
        sys.exit(1)
    print("All models match the base — no difference.")


if __name__ == "__main__":
    main()
