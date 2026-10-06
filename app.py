"""
NovaCart Order Analytics - Streamlit UI
Run:  streamlit run app.py
"""
import json

import pandas as pd
import streamlit as st

from pipeline import config, evidence
from pipeline.job import run_job, ConcurrentRunError
from pipeline.lakehouse import Lake, reset_lake

st.set_page_config(page_title="NovaCart Order Analytics", page_icon="🛒", layout="wide")


# ------------------------------------------------------------------ helpers
def lake():
    return Lake()


def has(l, name):
    try:
        return l.exists(name)
    except Exception:
        return False


def q(l, sql, params=()):
    return l.sql(sql, params)


def money(v):
    return f"${v:,.2f}"


EXPECTED = {  # handbook section 13: after batch 1 / after batch 2 (and batch 2 again)
    "bronze.orders rows": ("SELECT count(*) FROM bronze.orders", 1418, 2963),
    "bronze.order_items rows": ("SELECT count(*) FROM bronze.order_items", 1006, 2013),
    "silver.orders rows": ("SELECT count(*) FROM silver.orders", 495, 945),
    "silver.order_items rows": ("SELECT count(*) FROM silver.order_items", 971, 1933),
    "silver.dim_customer rows": ("SELECT count(*) FROM silver.dim_customer", 167, 167),
    "quarantine.order_items rows": ("SELECT count(*) FROM quarantine.order_items", 10, 18),
    "quarantine.customers rows": ("SELECT count(*) FROM quarantine.customers", 5, 5),
    "quarantine.payments rows": ("SELECT count(*) FROM quarantine.payments", 537, 5),
    "gold.fact_order_line rows": ("SELECT count(*) FROM gold.fact_order_line", 971, 1933),
    "gold.daily_revenue days": ("SELECT count(*) FROM gold.daily_revenue", 15, 30),
    "Total revenue USD": ("SELECT round(sum(revenue_usd), 2) FROM gold.daily_revenue", 150062.65, 295585.58),
    "gold.payment_mismatches rows": ("SELECT count(*) FROM gold.payment_mismatches", 42, 14),
}


def loaded_batches(l):
    if not has(l, "bronze.orders"):
        return []
    return list(q(l, "SELECT DISTINCT _batch_id FROM bronze.orders ORDER BY 1")["_batch_id"])


# ------------------------------------------------------------------ first start
@st.cache_resource
def _bootstrap():
    """On a fresh server (e.g. Streamlit Cloud) the lake is empty: load batch_1, batch_2, batch_2 once."""
    L0 = Lake()
    empty = not L0.exists("silver.orders")
    L0.close()
    if empty:
        for b in ["batch_1", "batch_2", "batch_2"]:
            run_job(b, log=lambda *_: None)
    return True


with st.spinner("Loading data for the first time…"):
    _bootstrap()

# ------------------------------------------------------------------ sidebar
st.sidebar.title("🛒 NovaCart")
st.sidebar.caption("Order Analytics · Medallion pipeline · Team Medallion")
page = st.sidebar.radio("Go to", ["Overview", "Run pipeline", "Gold dashboard", "Data quality",
                                  "Evidence (Delta)", "Data explorer", "Handbook check"])
L = lake()
batches = loaded_batches(L)
st.sidebar.markdown("**Batches loaded:** " + (", ".join(batches) if batches else "none yet"))
st.sidebar.markdown(f"**Input folder:** `{config.INPUT_PATH.name}/`")

# ================================================================== OVERVIEW
if page == "Overview":
    st.title("NovaCart Order Analytics")
    st.markdown("""
**Problem.** NovaCart's finance team gets different September revenue numbers from different spreadsheets.
Orders arrive as a log of status changes in two batches; batch 2 corrects batch 1 and even re-sends *older* versions.
Currencies are messy, FX rates exist only on business days, customers change tier and country, and some lines
point at orders or products that don't exist.

**What we built.** One automated pipeline that lands the raw files, cleans them without silently losing anything,
keeps only the latest correct version of every order, converts everything to USD with the right day's rate,
and produces one trusted revenue number. Running any batch again changes nothing.
""")
    st.graphviz_chart("""
digraph G { rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11];
  subgraph cluster_src { label="Input (8 files)"; style=dashed;
    f1 [label="orders_batch_N.csv\\norder_items_batch_N.jsonl", fillcolor="#eeeeee"];
    f2 [label="customers_changes.csv\\nproducts.csv · fx_rates.csv\\npayments.json", fillcolor="#eeeeee"]; }
  b [label="BRONZE\\nall text + lineage\\nreplaceWhere per batch", fillcolor="#e8c9a0"];
  s1 [label="SILVER L1\\nclean · cast · quarantine\\nwith reason", fillcolor="#d9dee7"];
  s2 [label="SILVER L2\\nlatest per order\\nMERGE if newer updated_at\\nSCD2 customers", fillcolor="#c5ccd8"];
  g [label="GOLD\\nfact_order_line\\ndaily · category · top10\\npayment mismatches", fillcolor="#f6e3a1"];
  dq [label="DQ report\\n+ 5-check gate", fillcolor="#cfe8d1"];
  q [label="quarantine.*", fillcolor="#f4c7c3"];
  ui [label="Streamlit UI", fillcolor="#e3d9f3"];
  f1 -> b; f2 -> b; b -> s1 -> s2 -> g -> dq -> ui; s1 -> q;
  job [label="Job: batch_id param · 2 retries · on_failure · 1 run at a time", shape=note, fillcolor="#ffffff"];
}""")
    if has(L, "gold.daily_revenue"):
        c = st.columns(4)
        c[0].metric("Revenue (USD)", money(L.scalar("SELECT coalesce(sum(revenue_usd),0) FROM gold.daily_revenue")))
        c[1].metric("Orders", L.scalar("SELECT count(*) FROM silver.orders"))
        c[2].metric("Order lines", L.scalar("SELECT count(*) FROM gold.fact_order_line"))
        c[3].metric("Payment mismatches", L.scalar("SELECT count(*) FROM gold.payment_mismatches"))
    else:
        st.info("No data yet. Open **Run pipeline** and run batch_1.")
    st.markdown("""
| Layer | Tables | Load pattern |
|---|---|---|
| Bronze | orders, order_items, customers, products, fx_rates, payments | batch files: replace this batch · snapshots: overwrite |
| Silver | orders (MERGE), order_items, dim_customer (SCD2), products, fx_rates, payments | orders incremental; others rebuilt |
| Quarantine | orders, order_items, customers, payments, products, fx_rates | rejected rows + reason + time |
| Gold | fact_order_line, daily_revenue, revenue_by_category, top10_customers, payment_mismatches | rebuilt from silver |
| DQ | dq_report, job_runs, pipeline_failures | per batch + stage |
""")

# ================================================================== RUN PIPELINE
elif page == "Run pipeline":
    st.title("Run the pipeline (the job)")
    st.caption("bronze → silver_orders → silver_others → gold → dq_checks · 2 retries per task · "
               "on_failure runs only if a task failed · one run at a time")
    available = sorted(p.name for p in config.INPUT_PATH.glob("orders_batch_*.csv"))
    st.write("Batch files in the input folder: " + ", ".join(f"`{a}`" for a in available))

    c = st.columns(5)
    action = None
    if c[0].button("🧹 Reset lake", use_container_width=True):
        action = "reset"
    if c[1].button("▶ Run batch_1", use_container_width=True):
        action = "batch_1"
    if c[2].button("▶ Run batch_2", use_container_width=True):
        action = "batch_2"
    if c[3].button("💥 Run batch_9 (failure demo)", use_container_width=True):
        action = "batch_9"
    if c[4].button("🎬 Full demo (reset → 1 → 2 → 2)", use_container_width=True):
        action = "demo"
    custom = st.text_input("…or any batch_id (job parameter)", value="")
    if st.button("Run with this batch_id") and custom:
        action = custom.strip()

    if action:
        log_lines = []
        box = st.empty()

        def log(msg):
            log_lines.append(str(msg))
            box.code("\n".join(log_lines[-40:]), language="text")

        try:
            if action == "reset":
                L.close()
                reset_lake()
                st.success("Lake reset: all layers dropped. Input files untouched.")
            else:
                todo = ["batch_1", "batch_2", "batch_2"] if action == "demo" else [action]
                if action == "demo":
                    L.close()
                    reset_lake()
                    log("lake reset")
                with st.spinner("Running…"):
                    for b in todo:
                        r = run_job(b, log=log, retry_wait=1 if b == "batch_9" else None)
                        (st.success if r["status"] == "SUCCESS" else st.error)(
                            f"Run {r['job_run_id']} ({b}): {r['status']}" + (f" — {r.get('error', '')}" if r["status"] != "SUCCESS" else ""))
            with st.expander("Full log", expanded=False):
                st.code("\n".join(log_lines), language="text")
        except ConcurrentRunError as e:
            st.warning(str(e))
        L = lake()

    if has(L, "dq.job_runs"):
        st.subheader("Run history (like the job's run page)")
        runs = q(L, """SELECT job_run_id, batch_id, min(started_at) AS started,
                        CASE WHEN sum(status='FAILED' AND task<>'on_failure') > 0 AND
                                  sum(task='on_failure' AND status='SUCCESS') > 0 THEN 'FAILED' ELSE 'SUCCESS' END AS status,
                        sum(status='FAILED') AS failed_attempts
                       FROM dq.job_runs GROUP BY job_run_id, batch_id ORDER BY started DESC""")
        st.dataframe(runs, use_container_width=True, hide_index=True)
        pick = st.selectbox("Task attempts for run", runs["job_run_id"])
        st.dataframe(q(L, "SELECT task, attempt, status, started_at, seconds, error FROM dq.job_runs "
                          "WHERE job_run_id = ? ORDER BY rowid", (pick,)), use_container_width=True, hide_index=True)
    if has(L, "silver.orders"):
        st.subheader("silver.orders MERGE history")
        st.dataframe(evidence.history(L).drop(columns=["operation_parameters"]), use_container_width=True, hide_index=True)

# ================================================================== GOLD DASHBOARD
elif page == "Gold dashboard":
    st.title("Gold: September revenue")
    if not has(L, "gold.fact_order_line"):
        st.info("Run the pipeline first.")
    else:
        fact = L.read("gold.fact_order_line")
        f1, f2, f3 = st.columns(3)
        cats = f1.multiselect("Category", sorted(fact["category"].unique()))
        ctry = f2.multiselect("Customer country (at order time)", sorted(fact["customer_country"].unique()))
        curr = f3.multiselect("Order currency", sorted(fact["currency"].unique()))
        v = fact[fact["is_revenue"] == 1]
        if cats:
            v = v[v["category"].isin(cats)]
        if ctry:
            v = v[v["customer_country"].isin(ctry)]
        if curr:
            v = v[v["currency"].isin(curr)]
        filtered = bool(cats or ctry or curr)
        total = L.scalar("SELECT sum(revenue_usd) FROM gold.daily_revenue") if not filtered else round(v["net_usd"].sum(), 2)
        k = st.columns(4)
        k[0].metric("Revenue (USD)", money(total or 0))
        k[1].metric("Revenue orders", v["order_id"].nunique())
        k[2].metric("Avg order value", money((total or 0) / max(v["order_id"].nunique(), 1)))
        k[3].metric("Return lines", int((v["qty"] < 0).sum()))

        st.subheader("Daily revenue (USD, order date in UTC)")
        daily = (L.read("gold.daily_revenue") if not filtered else
                 v.groupby("order_date", as_index=False)["net_usd"].sum().rename(columns={"net_usd": "revenue_usd"}))
        st.bar_chart(daily.set_index("order_date")["revenue_usd"])

        a, b = st.columns(2)
        with a:
            st.subheader("Revenue by category")
            cat = (L.read("gold.revenue_by_category") if not filtered else
                   v.groupby("category", as_index=False)["net_usd"].sum().rename(columns={"net_usd": "revenue_usd"}))
            cat = cat.sort_values("revenue_usd", ascending=False)
            st.bar_chart(cat.set_index("category")["revenue_usd"])
            st.dataframe(cat, use_container_width=True, hide_index=True)
        with b:
            st.subheader("Top 10 customers")
            if filtered:
                top = (v.groupby("customer_id", as_index=False)["net_usd"].sum().rename(columns={"net_usd": "revenue_usd"})
                       .sort_values("revenue_usd", ascending=False).head(10))
            else:
                top = L.read("gold.top10_customers")
            st.dataframe(top, use_container_width=True, hide_index=True)

        st.subheader("Payment mismatches (|paid − order total| > 0.50, order currency)")
        st.caption("Assumption: revenue orders only (14 rows). A literal reading also lists every cancelled and placed order (165).")
        st.dataframe(L.read("gold.payment_mismatches"), use_container_width=True, hide_index=True)

        with st.expander("Fact table (gold.fact_order_line)"):
            st.dataframe(fact, use_container_width=True, hide_index=True)

# ================================================================== DATA QUALITY
elif page == "Data quality":
    st.title("Data quality: nothing is dropped silently")
    if not has(L, "dq.dq_report"):
        st.info("Run the pipeline first.")
    else:
        b = st.selectbox("Batch", batches[::-1] or ["batch_1"])
        rep = q(L, "SELECT stage, rule, rows_in, rows_out, rows_quarantined, rows_flagged, run_ts FROM dq.dq_report "
                   "WHERE batch_id = ? ORDER BY CASE stage WHEN 'bronze' THEN 1 WHEN 'silver_orders' THEN 2 "
                   "WHEN 'silver_others' THEN 3 WHEN 'gold' THEN 4 ELSE 5 END, rowid", (b,))
        gates = rep[rep["stage"] == "gate"]
        if len(gates):
            st.subheader("Gate (05_dq_checks)")
            cols = st.columns(len(gates))
            for col, (_, r) in zip(cols, gates.iterrows()):
                col.metric(r["rule"].replace("_", " ").title(), "PASS ✅" if r["rows_out"] == 1 else "FAIL ❌")
        st.subheader("DQ report")
        st.dataframe(rep[rep["stage"] != "gate"], use_container_width=True, hide_index=True)

        if has(L, "silver.order_items"):
            st.subheader("Reconciliation")
            s = L.scalar
            items_in = s("SELECT count(*) FROM bronze.order_items")
            iq = s("SELECT count(*) FROM quarantine.order_items")
            il = s("SELECT count(*) FROM silver.order_items")
            cu_in = s("SELECT count(*) FROM bronze.customers")
            pay_in = s("SELECT count(*) FROM bronze.payments")
            st.markdown(f"""
* **Order items:** {items_in:,} bronze rows = {iq} quarantined + {items_in - iq - il} duplicates + {il:,} lines
* **Customers:** {cu_in} rows = {s('SELECT count(*) FROM quarantine.customers')} quarantined + exact duplicates + no-change rows + {s('SELECT count(*) FROM silver.dim_customer') - 1} versions (+1 unknown member)
* **Payments:** {pay_in:,} rows = {s('SELECT count(*) FROM quarantine.payments')} quarantined + {s('SELECT count(*) FROM silver.payments'):,} loaded
* **Revenue:** daily total {money(s('SELECT coalesce(sum(revenue_usd),0) FROM gold.daily_revenue'))} = category total {money(s('SELECT coalesce(sum(revenue_usd),0) FROM gold.revenue_by_category'))}
""")
        st.subheader("Quarantine tables")
        qt = st.selectbox("Table", ["quarantine.order_items", "quarantine.customers", "quarantine.payments",
                                    "quarantine.orders", "quarantine.products", "quarantine.fx_rates"])
        if has(L, qt):
            df = L.read(qt)
            st.write(f"{len(df)} rows")
            if len(df):
                st.dataframe(df.groupby("_reject_reason").size().reset_index(name="rows"), hide_index=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
        if has(L, "dq.pipeline_failures"):
            st.subheader("dq.pipeline_failures (written by on_failure)")
            st.dataframe(L.read("dq.pipeline_failures"), use_container_width=True, hide_index=True)

# ================================================================== EVIDENCE
elif page == "Evidence (Delta)":
    st.title("Reliability evidence on silver.orders")
    if not has(L, "silver.orders"):
        st.info("Run batch_1, batch_2 and batch_2 again first.")
    else:
        t = st.tabs(["1 History", "2 Time travel", "3 Schema enforcement", "4 OPTIMIZE", "5 Late version",
                     "6 Change feed", "7 SCD2 point-in-time", "8 FX fallback", "9 No duplicates"])
        with t[0]:
            st.code("DESCRIBE HISTORY silver.orders", language="sql")
            st.dataframe(evidence.history(L), use_container_width=True, hide_index=True)
            st.caption("One MERGE per run: 495/0 → 450/137 → 0/0 (the rerun changes nothing).")
        with t[1]:
            v1, counts, sample = evidence.time_travel(L)
            if v1 is not None:
                st.code(f"SELECT * FROM silver.orders VERSION AS OF {v1}", language="sql")
                st.dataframe(counts, hide_index=True)
                st.dataframe(sample, hide_index=True)
                st.caption("NC-100280 was paid after batch 1 and is delivered now.")
        with t[2]:
            st.write("Try to append one row with an extra column `unexpected_col`.")
            if st.button("Attempt bad write"):
                msg, before, after = evidence.schema_enforcement(L)
                st.error(msg)
                st.write(f"Row count before: **{before}**, after: **{after}**")
        with t[3]:
            st.code("OPTIMIZE gold.fact_order_line ZORDER BY (order_date)", language="sql")
            if st.button("Run OPTIMIZE"):
                h, v = evidence.optimize(L)
                st.success(f"Committed version {v}")
                st.dataframe(h, hide_index=True)
            st.markdown("Partition only very large tables on a low-cardinality column; Z-order/cluster on frequently "
                        "filtered columns like `order_date`. **VACUUM** deletes old files and breaks time travel to "
                        "those versions, so we don't run it here.")
        with t[4]:
            st.dataframe(evidence.late_version(L), use_container_width=True, hide_index=True)
            st.caption("Batch 2 sent a *paid* version dated 2 Sep, older than the stored *cancelled* (3 Sep). "
                       "MERGE condition `s.updated_at > t.updated_at` blocked it.")
        with t[5]:
            v2, summary, sample = evidence.change_feed(L)
            if v2 is not None:
                st.code(f"SELECT * FROM table_changes('silver.orders', {v2}, {v2})", language="sql")
                st.dataframe(summary, hide_index=True)
                st.dataframe(sample, hide_index=True)
        with t[6]:
            cid = st.text_input("Customer", "C0004")
            st.dataframe(q(L, "SELECT customer_sk, tier, country, valid_from, valid_to, is_current "
                              "FROM silver.dim_customer WHERE customer_id = ? ORDER BY valid_from", (cid,)), hide_index=True)
            if has(L, "gold.fact_order_line"):
                st.dataframe(q(L, "SELECT DISTINCT order_id, order_ts_utc, customer_sk, customer_tier, customer_country "
                                  "FROM gold.fact_order_line WHERE customer_id = ? ORDER BY order_ts_utc", (cid,)), hide_index=True)
            st.caption("Orders join the version valid at order time, never is_current.")
        with t[7]:
            if has(L, "gold.fact_order_line"):
                oid = st.text_input("Order", "NC-100133")
                st.dataframe(q(L, "SELECT order_id, line_no, order_date, currency, fx_rate, fx_rate_date, net_local, net_usd, "
                                  "order_status, is_revenue FROM gold.fact_order_line WHERE order_id = ?", (oid,)), hide_index=True)
                st.caption("Saturday 5 Sep has no rate → Friday 4 Sep's rate. "
                           f"Lines using an earlier rate: {L.scalar('SELECT count(*) FROM gold.fact_order_line WHERE fx_rate_date < order_date')}")
        with t[8]:
            st.dataframe(evidence.no_duplicates(L), hide_index=True)

# ================================================================== EXPLORER
elif page == "Data explorer":
    st.title("Data explorer")
    schema = st.selectbox("Layer", config.SCHEMAS)
    tables = [t for t in q(L, f"SELECT name FROM {schema}.sqlite_master WHERE type='table' ORDER BY name")["name"]
              if not t.startswith("__")]
    if not tables:
        st.info("No tables yet.")
    else:
        t = st.selectbox("Table", tables)
        df = L.read(f"{schema}.{t}")
        st.write(f"**{schema}.{t}** · {len(df):,} rows · {len(df.columns)} columns")
        search = st.text_input("Filter rows containing")
        if search:
            df = df[df.astype(str).apply(lambda r: r.str.contains(search, case=False, regex=False)).any(axis=1)]
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("Download CSV", df.to_csv(index=False), file_name=f"{schema}.{t}.csv")

# ================================================================== HANDBOOK CHECK
elif page == "Handbook check":
    st.title("Handbook check: expected vs actual")
    if not batches:
        st.info("Run the pipeline first.")
    else:
        idx = 1 if batches[-1] == "batch_1" else 2
        st.caption(f"Comparing with the handbook values **after {batches[-1]}**.")
        rows = []
        for name, (sql, e1, e2) in EXPECTED.items():
            exp = e1 if idx == 1 else e2
            try:
                act = L.scalar(sql)
            except Exception:
                act = None
            ok = act is not None and abs(float(act) - float(exp)) < 0.005
            rows.append({"measure": name, "expected": exp, "actual": act, "match": "✅" if ok else "❌"})
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        n = (df["match"] == "✅").sum()
        (st.success if n == len(df) else st.warning)(f"{n} of {len(df)} measures match the handbook.")
