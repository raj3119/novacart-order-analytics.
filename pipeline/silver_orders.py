"""02 SILVER ORDERS: clean -> quarantine -> keep latest per order -> MERGE (only newer updated_at wins)."""
import pandas as pd

from . import config
from .common import isnull, blank_to_none, parse_ts, split

ORDER_COLS = ["order_id", "customer_id", "order_ts_utc", "order_date", "status", "currency",
              "currency_derived", "shipping_country", "promo_code", "updated_at",
              "_batch_id", "_source_file", "_ingested_at"]
RAW_COLS = ["order_id", "customer_id", "order_ts", "status", "currency", "shipping_country",
            "updated_at", "promo_code", "_source_file", "_batch_id"]


def run(lake, batch_id, dq, log=print):
    raw = lake.sql("SELECT * FROM bronze.orders WHERE _batch_id = ?", (batch_id,))
    df = raw.copy()
    df["order_id_c"] = df["order_id"].map(blank_to_none)
    df["customer_id_c"] = df["customer_id"].map(blank_to_none)
    df["order_ts_utc"] = df["order_ts"].map(parse_ts)                       # 3 formats -> UTC
    df["updated_at_c"] = df["updated_at"].map(parse_ts)
    df["status_c"] = df["status"].map(lambda v: (blank_to_none(v) or "").lower() or None)
    df["shipping_country_c"] = df["shipping_country"].map(lambda v: (blank_to_none(v) or "").upper() or None)
    given = df["currency"].map(lambda v: (blank_to_none(v) or "").upper() or None)   # case-insensitive
    df["currency_derived"] = given.isna()
    df["currency_c"] = given.where(given.notna(), df["shipping_country_c"].map(config.COUNTRY_CURRENCY))
    df["promo_code_c"] = df["promo_code"].map(blank_to_none)

    rules = [
        (lambda r: isnull(r["order_id_c"]), "ORDER_ID_MISSING"),
        (lambda r: isnull(r["order_ts_utc"]), "ORDER_TS_INVALID"),
        (lambda r: isnull(r["updated_at_c"]), "UPDATED_AT_INVALID"),
        (lambda r: r["status_c"] not in config.VALID_STATUSES, "STATUS_INVALID"),
        (lambda r: r["currency_c"] not in config.VALID_CURRENCIES, "CURRENCY_UNKNOWN"),
    ]
    good, bad = split(df, rules, "silver_orders", dq, RAW_COLS)
    lake.replace_where("quarantine.orders", bad, "_batch_id", batch_id)     # replaced per batch

    # keep the newest version of each order inside the batch (MERGE needs one source row per key)
    good = good.sort_values(["order_id_c", "updated_at_c", "status_c"], ascending=[True, False, True], kind="stable")
    latest = good.drop_duplicates("order_id_c", keep="first")
    staged = pd.DataFrame({
        "order_id": latest["order_id_c"], "customer_id": latest["customer_id_c"],
        "order_ts_utc": latest["order_ts_utc"], "order_date": latest["order_ts_utc"].str[:10],
        "status": latest["status_c"], "currency": latest["currency_c"],
        "currency_derived": latest["currency_derived"].astype(int),
        "shipping_country": latest["shipping_country_c"], "promo_code": latest["promo_code_c"],
        "updated_at": latest["updated_at_c"], "_batch_id": latest["_batch_id"],
        "_source_file": latest["_source_file"], "_ingested_at": latest["_ingested_at"]})[ORDER_COLS]
    dq.add("silver_orders", "DEDUP_KEEP_LATEST_PER_ORDER", len(good), len(staged))
    dq.add("silver_orders", "CURRENCY_DERIVED_FROM_COUNTRY", len(staged), len(staged),
           flagged=int(staged["currency_derived"].sum()))

    lake.create_if_not_exists("silver.orders", staged.head(1) if len(staged) else pd.DataFrame(columns=ORDER_COLS))
    m = lake.merge("silver.orders", staged, key="order_id", newer_col="updated_at")
    log(f"MERGE silver.orders: inserted={m['numTargetRowsInserted']} updated={m['numTargetRowsUpdated']} "
        f"untouched={m['numTargetRowsLeftUntouched']} -> rows={lake.scalar('SELECT count(*) FROM silver.orders')}")
    dq.add("silver_orders", "MERGE_INSERTED", len(staged), m["numTargetRowsInserted"])
    dq.add("silver_orders", "MERGE_UPDATED", len(staged), m["numTargetRowsUpdated"])
    dq.add("silver_orders", "MERGE_LEFT_UNTOUCHED", len(staged), m["numTargetRowsLeftUntouched"])
    dq.flush("silver_orders")
    return m
