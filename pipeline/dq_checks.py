"""05 DQ GATE: any failed check raises -> the task fails -> on_failure runs."""
from . import config


class GateFailed(Exception):
    pass


def run(lake, batch_id, dq, log=print):
    src_orders = sum(1 for _ in open(config.orders_file(batch_id), encoding="utf-8")) - 1
    src_items = sum(1 for line in open(config.items_file(batch_id), encoding="utf-8") if line.strip())
    s = lake.scalar
    checks = [
        ("ONE_ROW_PER_ORDER",
         s("SELECT count(*) - count(DISTINCT order_id) FROM silver.orders") == 0),
        ("BRONZE_BATCH_LOADED_EXACTLY_ONCE",
         s("SELECT count(*) FROM bronze.orders WHERE _batch_id = ?", (batch_id,)) == src_orders and
         s("SELECT count(*) FROM bronze.order_items WHERE _batch_id = ?", (batch_id,)) == src_items),
        ("ONE_FACT_ROW_PER_ORDER_LINE",
         s("SELECT count(*) FROM (SELECT order_id, line_no, line_type FROM gold.fact_order_line "
           "GROUP BY 1,2,3 HAVING count(*) > 1)") == 0),
        ("ONE_CURRENT_ROW_PER_CUSTOMER",
         s("SELECT count(*) FROM (SELECT customer_id FROM silver.dim_customer GROUP BY customer_id "
           "HAVING sum(is_current) <> 1)") == 0),
        ("DAILY_EQUALS_CATEGORY_TOTAL",
         abs((s("SELECT coalesce(sum(revenue_usd),0) FROM gold.daily_revenue") or 0) -
             (s("SELECT coalesce(sum(revenue_usd),0) FROM gold.revenue_by_category") or 0)) < 0.05),
    ]
    failed = [n for n, ok in checks if not ok]
    for name, ok in checks:
        log(("PASS  " if ok else "FAIL  ") + name)
        dq.add("gate", name, 1, 1 if ok else 0, flagged=0 if ok else 1)
    dq.flush("gate")
    if failed:
        raise GateFailed(f"Data quality gate failed for {batch_id}: {failed}")
    log(f"All {len(checks)} gate checks passed for {batch_id}")
    return checks
