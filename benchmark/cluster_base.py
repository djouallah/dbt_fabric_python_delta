"""Re-cluster the base mart.fct_summary IN PLACE with a chosen sort (duckrun) — run AFTER Livy
has already built fct_summary_vorder from the un-clustered summary, so nobody can claim the
V-Order copy was pre-conditioned by duckrun's clustering.

Env: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, BASE_SORT ('auto' for the sort-key recommender, or a
column list like 'date, time'; default 'auto').
"""
import os

import duckrun

sort = (os.environ.get("BASE_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/mart",
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]},
                      read_only=False)
print(f"Re-clustering mart.fct_summary with '{clause}' ...", flush=True)
con.sql(f"create or replace table mart.fct_summary {clause} as select * from mart.fct_summary")
rows = con.sql("select count(*) from mart.fct_summary").fetchone()[0]
print(f"done — mart.fct_summary reclustered ({rows:,} rows)", flush=True)
