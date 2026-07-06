"""duckrun get_stats for the summary tables — console table + a markdown section in the
GitHub Actions job summary (so the XMLA report card shows both tables' physical layout:
rows / num_files / num_row_groups / avg_row_group / size_mb / vorder / compression).

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN (minted in the workflow).
"""
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Right-align these (numeric); everything else left-aligns.
_NUM = {"total_rows", "num_files", "num_row_groups", "avg_row_group", "size_mb"}
_FLOAT = {"avg_row_group", "size_mb"}


def _fmt(col, v):
    if v is None:
        return ""
    if col in _NUM:
        try:
            return f"{float(v):,.1f}" if col in _FLOAT else f"{int(v):,}"
        except (TypeError, ValueError):
            return str(v)
    return str(v)


def markdown(cols, rows):
    align = ["--:" if c in _NUM else ":--" for c in cols]
    out = ["## 📊 Table layout — duckrun `get_stats('fct_summary*')`", "",
           "| " + " | ".join(cols) + " |",
           "| " + " | ".join(align) + " |"]
    for r in rows:
        out.append("| " + " | ".join(_fmt(c, v) for c, v in zip(cols, r)) + " |")
    out += ["", "_`vorder` = Fabric V-Order flag; `avg_row_group` = rows per row group "
            "(smaller ⇒ finer granularity, usually faster Direct Lake cold transcode)._", ""]
    return "\n".join(out)


def main():
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/mart",
                          storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})
    rel = con.get_stats("fct_summary*")
    rel.show()  # console
    cols, rows = rel.columns, rel.fetchall()
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(markdown(cols, rows) + "\n")


if __name__ == "__main__":
    main()
