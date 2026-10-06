"""06 EVIDENCE: the Delta-style reliability proof on silver.orders (PDF Part 5 and Part 6)."""
import json

import pandas as pd

from .lakehouse import SchemaMismatchError


def history(lake, table="silver.orders"):
    h = lake.history(table)
    m = h["operation_metrics"].map(json.loads)
    h["inserted"] = m.map(lambda x: x.get("numTargetRowsInserted"))
    h["updated"] = m.map(lambda x: x.get("numTargetRowsUpdated"))
    h["source_rows"] = m.map(lambda x: x.get("numSourceRows"))
    return h[["version", "timestamp", "operation", "inserted", "updated", "source_rows", "operation_parameters"]]


def merge_versions(lake):
    h = lake.history("silver.orders")
    return list(h[h["operation"] == "MERGE"]["version"])


def time_travel(lake):
    v = merge_versions(lake)
    if not v:
        return None, None, None
    v1 = v[0]
    then = lake.read("silver.orders", version=v1)
    now = lake.read("silver.orders")
    counts = pd.DataFrame([{"state": f"after batch 1 (VERSION AS OF {v1})", "orders": len(then)},
                           {"state": "now", "orders": len(now)}])
    sample = pd.concat([then[then.order_id == "NC-100280"].assign(state="then"),
                        now[now.order_id == "NC-100280"].assign(state="now")])[["state", "order_id", "status", "updated_at"]]
    return v1, counts, sample


def schema_enforcement(lake):
    before = lake.scalar("SELECT count(*) FROM silver.orders")
    bad = lake.read("silver.orders").head(1).assign(unexpected_col="x")
    try:
        lake.append("silver.orders", bad)
        msg = "UNEXPECTED: write succeeded"
    except SchemaMismatchError as e:
        msg = f"Write rejected -> {e}"
    after = lake.scalar("SELECT count(*) FROM silver.orders")
    return msg, before, after


def optimize(lake):
    v = lake.optimize("gold.fact_order_line", ["order_date"])
    return history(lake, "gold.fact_order_line").tail(2), v


def late_version(lake, order_id="NC-100039"):
    b = lake.sql("SELECT 'bronze' AS layer, _batch_id, status, updated_at FROM bronze.orders WHERE order_id = ? "
                 "ORDER BY updated_at", (order_id,))
    s = lake.sql("SELECT 'silver (kept)' AS layer, _batch_id, status, updated_at FROM silver.orders WHERE order_id = ?",
                 (order_id,))
    return pd.concat([b, s], ignore_index=True)


def change_feed(lake):
    v = merge_versions(lake)
    if len(v) < 2:
        return None, None, None
    v2 = v[1]
    ch = lake.table_changes("silver.orders", v2, v2)
    summary = ch.groupby("_change_type").size().reset_index(name="rows")
    sample = ch[ch.order_id == "NC-100280"][["_change_type", "order_id", "status", "updated_at"]]
    return v2, summary, sample


def no_duplicates(lake):
    return lake.sql("""
        SELECT count(*) AS rows, count(DISTINCT order_id) AS distinct_orders FROM silver.orders""")
