"""03 SILVER OTHERS: products, FX, order items, payments, customer SCD Type 2 (all rebuilt each run)."""
import pandas as pd

from . import config
from .common import isnull, blank_to_none, parse_ts, split, try_dec, try_int

STAGE = "silver_others"


def _upper(v):
    s = blank_to_none(v)
    return s.upper() if s else None


def _lower(v):
    s = blank_to_none(v)
    return s.lower() if s else None


def run(lake, batch_id, dq, log=print):
    order_ids = set(lake.sql("SELECT order_id FROM silver.orders")["order_id"])

    # ---------------- products
    p = lake.read("bronze.products")
    p["product_id_c"] = p["product_id"].map(blank_to_none)
    good, bad = split(p, [(lambda r: isnull(r["product_id_c"]), "PRODUCT_ID_MISSING")], STAGE, dq,
                      ["product_id", "sku", "product_name", "category", "list_price_usd", "_source_file"])
    lake.overwrite("quarantine.products", bad)
    good = good.drop_duplicates("product_id_c")
    products = pd.DataFrame({"product_id": good["product_id_c"], "sku": good["sku"].map(blank_to_none),
                             "product_name": good["product_name"].map(blank_to_none),
                             "category": good["category"].map(blank_to_none),
                             "list_price_usd": good["list_price_usd"].map(lambda v: try_dec(v, 2))})
    lake.overwrite("silver.products", products)
    known_products = set(products["product_id"])

    # ---------------- FX rates (business days only; USD = 1 applied in gold)
    fx = lake.read("bronze.fx_rates")
    fx["rate_date_c"] = fx["rate_date"].map(lambda v: (parse_ts(v) or "")[:10] or None)
    fx["currency_c"] = fx["currency"].map(_upper)
    fx["rate_c"] = fx["rate_to_usd"].map(lambda v: try_dec(v, 6))
    good, bad = split(fx, [(lambda r: isnull(r["rate_date_c"]) or isnull(r["currency_c"]) or isnull(r["rate_c"])
                            or r["rate_c"] <= 0, "FX_ROW_INVALID")], STAGE, dq,
                      ["rate_date", "currency", "rate_to_usd", "_source_file"])
    lake.overwrite("quarantine.fx_rates", bad)
    good = good.drop_duplicates(["rate_date_c", "currency_c"])
    lake.overwrite("silver.fx_rates", pd.DataFrame({"rate_date": good["rate_date_c"], "currency": good["currency_c"],
                                                    "rate_to_usd": good["rate_c"]}))

    # ---------------- order items (all loaded batches)
    it = lake.read("bronze.order_items")
    it["order_id_c"] = it["order_id"].map(blank_to_none)
    it["line_no_c"] = it["line_no"].map(try_int)
    it["product_id_c"] = it["product_id"].map(blank_to_none)
    it["qty_c"] = it["qty"].map(try_int)
    it["unit_price_c"] = it["unit_price"].map(lambda v: try_dec(v, 2))
    it["discount_null"] = it["discount_pct"].map(lambda v: blank_to_none(v) is None)
    it["discount_c"] = [0 if is_null else try_dec(v, 2) for v, is_null in zip(it["discount_pct"], it["discount_null"])]
    it["line_type_c"] = it["line_type"].map(lambda v: _lower(v) or "sale")
    it["batch_no"] = it["_batch_id"].map(config.batch_no)
    dq.add(STAGE, "ITEM_NULL_DISCOUNT_SET_TO_0", len(it), len(it), flagged=int(it["discount_null"].sum()))

    item_rules = [
        (lambda r: isnull(r["order_id_c"]) or isnull(r["line_no_c"]), "ITEM_KEY_MISSING"),
        (lambda r: isnull(r["qty_c"]) or r["qty_c"] == 0 or isnull(r["unit_price_c"]) or r["unit_price_c"] < 0,
         "QTY_OR_PRICE_INVALID"),
        (lambda r: isnull(r["discount_c"]) or r["discount_c"] < 0 or r["discount_c"] > 100, "DISCOUNT_INVALID"),
        (lambda r: r["order_id_c"] not in order_ids, "ORDER_NOT_FOUND"),      # checked against ALL loaded orders
    ]
    good, bad = split(it, item_rules, STAGE, dq,
                      ["order_id", "line_no", "product_id", "qty", "unit_price", "discount_pct", "line_type",
                       "attributes", "_source_file", "_batch_id"])
    lake.overwrite("quarantine.order_items", bad)

    # one row per order line; a line repeated in a later batch replaces the earlier copy
    good = good.sort_values(["order_id_c", "line_no_c", "line_type_c", "batch_no"],
                            ascending=[True, True, True, False], kind="stable")
    dedup = good.drop_duplicates(["order_id_c", "line_no_c", "line_type_c"], keep="first")
    dq.add(STAGE, "ITEM_DEDUP_LATEST_BATCH_WINS", len(good), len(dedup))
    items = pd.DataFrame({
        "order_id": dedup["order_id_c"], "line_no": dedup["line_no_c"], "line_type": dedup["line_type_c"],
        "product_id": dedup["product_id_c"], "qty": dedup["qty_c"], "unit_price": dedup["unit_price_c"],
        "discount_pct": dedup["discount_c"], "attributes": dedup["attributes"],
        "is_unknown_product": (~dedup["product_id_c"].isin(known_products)).astype(int),
        "_batch_id": dedup["_batch_id"], "_source_file": dedup["_source_file"]})
    lake.overwrite("silver.order_items", items)
    dq.add(STAGE, "ITEM_UNKNOWN_PRODUCT_FLAGGED", len(items), len(items), flagged=int(items["is_unknown_product"].sum()))

    # ---------------- payments (whole-month snapshot)
    pay = lake.read("bronze.payments")
    pay["payment_id_c"] = pay["payment_id"].map(blank_to_none)
    pay["order_id_c"] = pay["order_id"].map(blank_to_none)
    pay["amount_c"] = pay["amount"].map(lambda v: try_dec(v, 2))
    pay["currency_c"] = pay["currency"].map(_upper)
    pay["status_c"] = pay["status"].map(_lower)
    pay["paid_at_c"] = pay["paid_at"].map(parse_ts)
    pay_rules = [
        (lambda r: isnull(r["payment_id_c"]), "PAYMENT_ID_MISSING"),
        (lambda r: isnull(r["amount_c"]) or r["amount_c"] < 0, "AMOUNT_INVALID"),
        (lambda r: r["status_c"] not in config.PAY_STATUSES, "PAY_STATUS_INVALID"),
        (lambda r: r["order_id_c"] not in order_ids, "PAY_ORDER_NOT_FOUND"),
    ]
    good, bad = split(pay, pay_rules, STAGE, dq,
                      ["payment_id", "order_id", "method", "amount", "currency", "status", "paid_at", "gateway_ref",
                       "_source_file"])
    lake.overwrite("quarantine.payments", bad)
    good = good.drop_duplicates("payment_id_c")
    lake.overwrite("silver.payments", pd.DataFrame({
        "payment_id": good["payment_id_c"], "order_id": good["order_id_c"], "method": good["method"].map(blank_to_none),
        "amount": good["amount_c"], "currency": good["currency_c"], "status": good["status_c"],
        "paid_at": good["paid_at_c"], "gateway_ref": good["gateway_ref"].map(blank_to_none)}))

    # ---------------- customers: SCD Type 2
    cu = lake.read("bronze.customers")
    cu["customer_id_c"] = cu["customer_id"].map(blank_to_none)
    cu["updated_at_c"] = cu["updated_at"].map(parse_ts)
    cu["tier_c"] = cu["tier"].map(_upper)
    cu["country_c"] = cu["country"].map(_upper)
    good, bad = split(cu, [(lambda r: isnull(r["customer_id_c"]), "CUSTOMER_ID_MISSING"),
                           (lambda r: isnull(r["updated_at_c"]), "CUSTOMER_UPDATED_AT_MISSING")], STAGE, dq,
                      ["customer_id", "full_name", "email", "tier", "country", "updated_at", "_source_file"])
    lake.overwrite("quarantine.customers", bad)

    dist = good.drop_duplicates(["customer_id_c", "updated_at_c"])
    dq.add(STAGE, "CUSTOMER_EXACT_DUPLICATES_DROPPED", len(good), len(dist))
    dist = dist.sort_values(["customer_id_c", "updated_at_c"], kind="stable")
    g = dist.groupby("customer_id_c", sort=False)
    dist["prev_tier"] = g["tier_c"].shift(1)
    dist["prev_country"] = g["country_c"].shift(1)
    dist["is_first"] = g.cumcount() == 0
    changed = (dist["tier_c"] != dist["prev_tier"]) | (dist["country_c"] != dist["prev_country"])
    versions = dist[dist["is_first"] | changed].copy()          # no tier/country change -> no new version
    dq.add(STAGE, "CUSTOMER_SCD2_NO_CHANGE_ROWS_SKIPPED", len(dist), len(versions))

    gv = versions.groupby("customer_id_c", sort=False)
    nxt = gv["updated_at_c"].shift(-1)
    versions["valid_from"] = versions["updated_at_c"].where(~versions["is_first"], config.LOW_DATE)
    versions["valid_to"] = nxt.fillna(config.HIGH_DATE)
    versions["is_current"] = nxt.isna().astype(int)
    versions = versions.sort_values(["customer_id_c", "valid_from"], kind="stable")
    versions["customer_sk"] = range(1, len(versions) + 1)
    dim = pd.DataFrame({
        "customer_sk": versions["customer_sk"], "customer_id": versions["customer_id_c"],
        "full_name": versions["full_name"].map(blank_to_none), "email": versions["email"].map(blank_to_none),
        "tier": versions["tier_c"], "country": versions["country_c"], "valid_from": versions["valid_from"],
        "valid_to": versions["valid_to"], "is_current": versions["is_current"],
        "source_updated_at": versions["updated_at_c"]})
    unknown = pd.DataFrame([{"customer_sk": -1, "customer_id": "UNKNOWN", "full_name": "Unknown customer",
                             "email": None, "tier": "UNKNOWN", "country": "UNKNOWN", "valid_from": config.LOW_DATE,
                             "valid_to": config.HIGH_DATE, "is_current": 1, "source_updated_at": None}])
    lake.overwrite("silver.dim_customer", pd.concat([unknown, dim], ignore_index=True))

    dq.flush(STAGE)
    log("silver: order_items={} dim_customer={} payments={} | quarantine: items={} customers={} payments={}".format(
        *[lake.scalar(f"SELECT count(*) FROM {t}") for t in
          ["silver.order_items", "silver.dim_customer", "silver.payments",
           "quarantine.order_items", "quarantine.customers", "quarantine.payments"]]))
