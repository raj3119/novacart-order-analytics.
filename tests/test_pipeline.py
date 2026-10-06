"""
End-to-end acceptance test against the handbook numbers.
  python -m pytest -q        (or)        python tests/test_pipeline.py
Uses a temporary lake so your real lake/ folder is not touched.
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config  # noqa: E402


def _run_all():
    tmp = Path(tempfile.mkdtemp()) / "lake"
    config.LAKE_PATH = tmp
    from pipeline.job import run_job
    from pipeline.lakehouse import Lake
    snaps = {}
    for label, b in [("b1", "batch_1"), ("b2", "batch_2"), ("b2_again", "batch_2")]:
        r = run_job(b, log=lambda *_: None)
        assert r["status"] == "SUCCESS", r
        L = Lake(tmp)
        snaps[label] = {t: L.scalar(f"SELECT count(*) FROM {t}") for t in
                        ["bronze.orders", "bronze.order_items", "silver.orders", "silver.order_items",
                         "silver.dim_customer", "quarantine.order_items", "quarantine.customers",
                         "quarantine.payments", "gold.fact_order_line", "gold.daily_revenue", "gold.payment_mismatches"]}
        snaps[label]["revenue"] = round(L.scalar("SELECT sum(revenue_usd) FROM gold.daily_revenue"), 2)
        L.close()
    r9 = run_job("batch_9", log=lambda *_: None, retry_wait=0)
    return tmp, snaps, r9


TMP, SNAPS, R9 = _run_all()


def test_batch_1():
    s = SNAPS["b1"]
    assert (s["bronze.orders"], s["silver.orders"], s["silver.order_items"], s["quarantine.payments"]) == (1418, 495, 971, 537)
    assert s["revenue"] == 150062.65 and s["gold.payment_mismatches"] == 42 and s["gold.daily_revenue"] == 15


def test_batch_2():
    s = SNAPS["b2"]
    assert (s["bronze.orders"], s["bronze.order_items"], s["silver.orders"], s["silver.order_items"]) == (2963, 2013, 945, 1933)
    assert (s["silver.dim_customer"], s["quarantine.order_items"], s["quarantine.customers"], s["quarantine.payments"]) == (167, 18, 5, 5)
    assert s["revenue"] == 295585.58 and s["gold.payment_mismatches"] == 14 and s["gold.daily_revenue"] == 30


def test_rerun_changes_nothing():
    assert SNAPS["b2"] == SNAPS["b2_again"]


def test_merge_metrics_and_late_version():
    from pipeline.lakehouse import Lake
    from pipeline import evidence
    L = Lake(TMP)
    h = evidence.history(L)
    merges = h[h.operation == "MERGE"][["inserted", "updated"]].values.tolist()
    assert merges == [[495, 0], [450, 137], [0, 0]]
    assert L.scalar("SELECT status FROM silver.orders WHERE order_id = 'NC-100039'") == "cancelled"
    assert L.scalar("SELECT status FROM silver.orders WHERE order_id = 'NC-100280'") == "delivered"


def test_scd2_point_in_time_and_fx():
    from pipeline.lakehouse import Lake
    L = Lake(TMP)
    assert L.scalar("SELECT DISTINCT customer_country FROM gold.fact_order_line WHERE order_id = 'NC-100369'") == "IN"
    assert L.scalar("SELECT DISTINCT customer_country FROM gold.fact_order_line WHERE order_id = 'NC-100632'") == "DE"
    assert L.scalar("SELECT DISTINCT fx_rate_date FROM gold.fact_order_line WHERE order_id = 'NC-100133'") == "2026-09-04"
    assert L.scalar("SELECT count(*) FROM gold.fact_order_line WHERE customer_sk = -1") == 39
    assert L.scalar("SELECT count(*) FROM gold.fact_order_line WHERE fx_rate_date < order_date") == 434


def test_schema_enforcement():
    from pipeline.lakehouse import Lake
    from pipeline import evidence
    msg, before, after = evidence.schema_enforcement(Lake(TMP))
    assert "rejected" in msg and before == after == 945


def test_failure_path():
    from pipeline.lakehouse import Lake
    assert R9["status"] == "FAILED"
    L = Lake(TMP)
    assert L.scalar("SELECT count(*) FROM dq.job_runs WHERE batch_id='batch_9' AND task='bronze' AND status='FAILED'") == 3
    assert L.scalar("SELECT count(*) FROM dq.pipeline_failures WHERE batch_id='batch_9'") == 1


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
