"""
04 GOLD: join 4 things -> calculate 3 things -> one fact table -> 4 reports. Rebuilt from silver every run.

net_local  = qty x unit_price x (1 - discount_pct/100)        (null discount = 0)
fx_rate    = rate on the order date (UTC), else the most recent earlier rate; USD = 1
net_usd    = net_local x fx_rate
is_revenue = current order status in (paid, shipped, delivered)
"""
import bisect
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from . import config

STAGE = "gold"
Q4, Q2 = Decimal("0.0001"), Decimal("0.01")


def D(v):
    return Decimal(str(v)) if v is not None and v == v else None


def run(lake, batch_id, dq, log=print):
    items = lake.read("silver.order_items")
    orders = lake.read("silver.orders")
    products = lake.read("silver.products")[["product_id", "category"]]
    fx = lake.read("silver.fx_rates")
    dim = lake.read("silver.dim_customer")

    # (2) FX as-of lookup: per currency, sorted dates -> latest rate on or before the order date
    fx_by_cur = {}
    for cur, grp in fx.sort_values("rate_date").groupby("currency"):
        fx_by_cur[cur] = (list(grp["rate_date"]), list(grp["rate_to_usd"]))

    def fx_lookup(cur, day):
        if cur == "USD":
            return Decimal("1"), day
        dates, rates = fx_by_cur.get(cur, ([], []))
        i = bisect.bisect_right(dates, day) - 1
        return (D(rates[i]), dates[i]) if i >= 0 else (None, None)

    # (3) point-in-time customer versions (never is_current)
    versions = {}
    for r in dim[dim["customer_sk"] != -1].itertuples(index=False):
        versions.setdefault(r.customer_id, []).append(r)

    def customer_at(cid, ts):
        for v in versions.get(cid, []):
            if v.valid_from <= ts < v.valid_to:
                return v.customer_sk, v.tier, v.country
        return -1, "UNKNOWN", "UNKNOWN"

    o = orders.set_index("order_id")
    cat = dict(zip(products["product_id"], products["category"]))
    rows = []
    for it in items.itertuples(index=False):
        od = o.loc[it.order_id]                                            # (1) order
        rate, rate_date = fx_lookup(od["currency"], od["order_date"])      # (2) FX
        sk, tier, country = customer_at(od["customer_id"], od["order_ts_utc"])   # (3) customer
        category = cat.get(it.product_id) or "UNKNOWN"                     # (4) product
        disc = D(it.discount_pct) if it.discount_pct is not None and it.discount_pct == it.discount_pct else Decimal(0)
        net_local = (Decimal(int(it.qty)) * D(it.unit_price) * (1 - disc / 100)).quantize(Q4, ROUND_HALF_UP)
        net_usd = (net_local * rate).quantize(Q4, ROUND_HALF_UP) if rate is not None else None
        rows.append({
            "order_id": it.order_id, "line_no": int(it.line_no), "line_type": it.line_type,
            "order_date": od["order_date"], "order_ts_utc": od["order_ts_utc"], "customer_id": od["customer_id"],
            "customer_sk": int(sk), "customer_tier": tier, "customer_country": country,
            "product_id": it.product_id, "category": category, "is_unknown_product": int(it.is_unknown_product),
            "qty": int(it.qty), "unit_price": D(it.unit_price), "discount_pct": disc, "currency": od["currency"],
            "net_local": net_local, "fx_rate": rate, "fx_rate_date": rate_date, "net_usd": net_usd,
            "order_status": od["status"], "is_revenue": int(od["status"] in config.REVENUE_STATUSES)})
    fact = pd.DataFrame(rows).sort_values(["order_date", "order_id", "line_no"], kind="stable")
    lake.overwrite("gold.fact_order_line", fact)

    n = len(fact)
    dq.add(STAGE, "FACT_ROWS", n, n)
    dq.add(STAGE, "FACT_UNKNOWN_CUSTOMER_SK_MINUS_1", n, n, flagged=int((fact["customer_sk"] == -1).sum()))
    dq.add(STAGE, "FACT_UNKNOWN_PRODUCT_CATEGORY", n, n, flagged=int((fact["category"] == "UNKNOWN").sum()))
    dq.add(STAGE, "FACT_NOT_REVENUE_STATUS", n, n, flagged=int((fact["is_revenue"] == 0).sum()))
    dq.add(STAGE, "FACT_RETURN_LINES", n, n, flagged=int((fact["qty"] < 0).sum()))
    dq.add(STAGE, "FACT_FX_EARLIER_RATE_USED", n, n, flagged=int((fact["fx_rate_date"] < fact["order_date"]).sum()))
    dq.add(STAGE, "FACT_FX_RATE_MISSING", n, n, flagged=int(fact["fx_rate"].isna().sum()))

    rev = fact[fact["is_revenue"] == 1]

    def agg(by):
        out = []
        for key, g in rev.groupby(by, sort=True):
            out.append({by: key, "revenue_usd": sum(g["net_usd"], Decimal(0)).quantize(Q2, ROUND_HALF_UP),
                        "orders": g["order_id"].nunique()})
        return pd.DataFrame(out)

    daily = agg("order_date")
    lake.overwrite("gold.daily_revenue", daily)
    by_cat = agg("category").sort_values("revenue_usd", ascending=False)
    lake.overwrite("gold.revenue_by_category", by_cat)

    cur = dim[(dim["is_current"] == 1) & (dim["customer_sk"] != -1)][["customer_id", "full_name", "tier"]]
    top = agg("customer_id").merge(cur.rename(columns={"tier": "current_tier"}), on="customer_id", how="left")
    top = top.sort_values(["revenue_usd", "customer_id"], ascending=[False, True]).head(10)
    top.insert(0, "rank", range(1, len(top) + 1))
    lake.overwrite("gold.top10_customers", top[["rank", "customer_id", "full_name", "current_tier", "revenue_usd", "orders"]])

    # payment mismatches: order currency; failed = 0; refunds subtracted
    scope = rev if config.MISMATCH_REVENUE_ORDERS_ONLY else fact
    pays = lake.read("silver.payments")
    paid = {}
    cnt = {}
    for p in pays.itertuples(index=False):
        sign = {"success": 1, "refunded": -1}.get(p.status, 0)
        paid[p.order_id] = paid.get(p.order_id, Decimal(0)) + sign * D(p.amount)
        cnt[p.order_id] = cnt.get(p.order_id, 0) + 1
    mism = []
    for (oid, curr, status), g in scope.groupby(["order_id", "currency", "order_status"]):
        total = sum(g["net_local"], Decimal(0)).quantize(Q2, ROUND_HALF_UP)
        pn = paid.get(oid, Decimal(0)).quantize(Q2, ROUND_HALF_UP)
        diff = pn - total
        if abs(diff) > Decimal(str(config.MISMATCH_TOLERANCE)):
            mism.append({"order_id": oid, "currency": curr, "order_status": status, "order_net_local": total,
                         "paid_net": pn, "payment_rows": cnt.get(oid, 0), "difference": diff})
    mism_df = pd.DataFrame(mism, columns=["order_id", "currency", "order_status", "order_net_local", "paid_net",
                                          "payment_rows", "difference"])
    if len(mism_df):
        mism_df = mism_df.assign(_abs=mism_df["difference"].map(abs)).sort_values("_abs", ascending=False).drop(columns="_abs")
    lake.overwrite("gold.payment_mismatches", mism_df)
    dq.add(STAGE, "PAYMENT_MISMATCHES", scope["order_id"].nunique(), scope["order_id"].nunique(), flagged=len(mism_df))
    dq.flush(STAGE)

    total = sum(daily["revenue_usd"], Decimal(0)) if len(daily) else Decimal(0)   # = category total (gate 5)
    log(f"gold: fact_order_line={n} days={len(daily)} total_revenue_usd={total:,.2f} mismatches={len(mism_df)}")
    return total
