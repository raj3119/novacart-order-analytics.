<div align="center">

# 🛒 NovaCart Order Analytics

**A medallion data pipeline that turns messy, multi-batch e-commerce files into one trusted revenue number.**

![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-data%20processing-150458?logo=pandas&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-UI-FF4B4B?logo=streamlit&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-lakehouse-003B57?logo=sqlite&logoColor=white)
![Tests](https://img.shields.io/badge/tests-7%20passing-brightgreen)
![Architecture](https://img.shields.io/badge/architecture-bronze%20%E2%86%92%20silver%20%E2%86%92%20gold-c58a00)

**September revenue: `$295,585.58` · 945 orders · 1,933 order lines · rerun-safe · 5/5 quality checks passing**

[Problem](#-the-problem) · [Architecture](#%EF%B8%8F-architecture) · [Workflow](#-how-a-batch-flows-through-the-pipeline) · [Insights](#-business-insights) · [Reliability](#%EF%B8%8F-reliability-evidence) · [Run it](#-quick-start)

</div>

---

## 📌 The problem

NovaCart's finance team was getting **different September revenue numbers from different spreadsheets**. The raw data shows why:

| What the data looks like | Why it breaks a simple report |
|---|---|
| Orders arrive as a **log of status changes**: 1,418 rows hold only 495 orders | Counting rows overstates revenue |
| Data arrives in **two batches**, and batch 2 corrects batch 1 | "Load the newest file" is not enough |
| Batch 2 even re-sends **10 versions older** than ones already loaded | A cancelled order can come back as *paid* |
| Timestamps come in **3 formats** (`Z`, `+05:30`, `dd/MM/yyyy HH:mm`) | Orders land on the wrong day |
| Currency is **blank (194 rows) or lower-case (145 rows)** | Amounts can't be converted |
| FX rates exist **only on business days** | Weekend orders have no rate |
| Customers **change tier and country** mid-month | Reports use today's value, not the true one |
| **18 lines** for orders that don't exist, **14 lines** with unknown products, **5 payments** for missing orders | Rows get dropped silently or totals are wrong |

**Goal:** one automated pipeline that keeps the latest correct version of every order, converts everything to USD with the right day's rate, never loses a row silently, and gives **exactly the same answer when re-run**.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    subgraph SRC["📂 Source files (8)"]
        A1["orders_batch_N.csv"]
        A2["order_items_batch_N.jsonl"]
        A3["customers_changes.csv<br/>products.csv<br/>fx_rates.csv<br/>payments.json"]
    end

    subgraph BRONZE["🥉 Bronze: keep as received"]
        B["All values as text<br/>+ _source_file, _batch_id, _ingested_at<br/>replace-per-batch (rerun-safe)"]
    end

    subgraph SILVER["🥈 Silver: clean and conform"]
        S1["Parse · cast · normalise<br/>first failing rule → quarantine"]
        S2["Latest version per order<br/>MERGE only if updated_at is newer"]
        S3["Customer history (SCD Type 2)<br/>products · FX · payments"]
    end

    subgraph GOLD["🥇 Gold: business answers"]
        G1["fact_order_line"]
        G2["daily_revenue · revenue_by_category<br/>top10_customers · payment_mismatches"]
    end

    Q["🚫 Quarantine<br/>rejected rows + reason"]
    DQ["✅ DQ report<br/>+ 5-check gate"]
    UI["📊 Streamlit UI"]

    A1 & A2 & A3 --> B --> S1 --> S2 --> S3 --> G1 --> G2 --> DQ --> UI
    S1 -.-> Q
```

| Layer | Purpose | Tables | Load pattern |
|---|---|---|---|
| 🥉 **Bronze** | Exact copy of what arrived, for replay and audit | `orders`, `order_items`, `customers`, `products`, `fx_rates`, `payments` | Batch files: replace this batch's rows · Snapshots: overwrite |
| 🥈 **Silver** | Clean, typed, one row per business key | `orders` (MERGE), `order_items`, `dim_customer` (SCD2), `products`, `fx_rates`, `payments` | `orders` incremental; others rebuilt |
| 🚫 **Quarantine** | Bad rows kept with original values, reason and time | `orders`, `order_items`, `customers`, `payments`, `products`, `fx_rates` | Rebuilt / per batch |
| 🥇 **Gold** | Business-ready answers | `fact_order_line`, `daily_revenue`, `revenue_by_category`, `top10_customers`, `payment_mismatches` | Rebuilt from silver |
| ✅ **DQ** | Proof that every rule ran | `dq_report`, `job_runs`, `pipeline_failures` | Per batch and stage |

---

## 🔄 How a batch flows through the pipeline

The pipeline runs as a **job** with one parameter (`batch_id`), five tasks in order, **2 retries per task**, a failure path, and only one run at a time.

```mermaid
flowchart TD
    START(["▶ run_job(batch_id)"]) --> T1["01 bronze<br/>land files as text"]
    T1 --> T2["02 silver_orders<br/>clean → quarantine → latest → MERGE"]
    T2 --> T3["03 silver_others<br/>items · payments · FX · SCD2 customers"]
    T3 --> T4["04 gold<br/>fact table + 4 reports"]
    T4 --> T5{"05 dq_checks<br/>5 gate checks"}
    T5 -->|all pass| OK(["✅ SUCCESS"])

    T1 & T2 & T3 & T4 & T5 -.->|"fails 3× (1 try + 2 retries)"| F["⛔ downstream tasks SKIPPED"]
    F --> ONF["on_failure<br/>writes dq.pipeline_failures"]
    ONF --> FAIL(["❌ FAILED"])
```

### The one rule that makes it correct

```sql
MERGE INTO silver.orders AS t
USING staged_orders AS s           -- newest row per order inside this batch
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *   -- only newer wins
WHEN NOT MATCHED THEN INSERT *
```

This single condition handles three problems at once:

```mermaid
flowchart LR
    IN["Incoming order version"] --> C{"Order already<br/>in silver?"}
    C -->|No| INS["➕ INSERT<br/>new order"]
    C -->|Yes| D{"incoming updated_at<br/>&gt; stored updated_at?"}
    D -->|Yes| UPD["✏️ UPDATE<br/>correction applied"]
    D -->|No| SKIP["🛑 KEEP STORED<br/>late older version blocked<br/>or rerun → no change"]
```

| Run | Inserted | Updated | Left untouched | `silver.orders` rows |
|---|---:|---:|---:|---:|
| batch_1 | 495 | 0 | 0 | 495 |
| batch_2 | 450 | 137 | **10 late older versions** | 945 |
| batch_2 again | 0 | 0 | 597 | **945 (no change)** |

### Gold: join 4 things → calculate 3 things

```mermaid
flowchart LR
    L["Order line"] --> J1["① + order<br/>status, currency, time"]
    J1 --> J2["② + FX rate<br/>order date, else latest earlier"]
    J2 --> J3["③ + customer version<br/>valid at order time"]
    J3 --> J4["④ + product<br/>category or UNKNOWN"]
    J4 --> C1["net_local = qty × price × (1 − discount/100)"]
    C1 --> C2["net_usd = net_local × fx_rate"]
    C2 --> C3["is_revenue = status ∈ paid, shipped, delivered"]
    C3 --> F["gold.fact_order_line"]
```

---

## 📈 Business insights

> All figures are September 2026 after both batches. Revenue counts only **paid, shipped and delivered** orders, converted to USD.

### Daily revenue (USD)

```mermaid
xychart-beta
    title "Daily revenue, September 2026 (USD)"
    x-axis "Day" [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
    y-axis "Revenue (USD)" 0 --> 14000
    bar [12574, 11074, 13112, 7669, 9348, 8573, 11442, 7022, 9605, 8332, 8845, 10836, 9137, 13135, 9191, 9052, 10065, 10284, 8014, 10418, 6884, 7876, 12151, 9353, 10300, 11817, 8371, 11247, 9865, 9996]
```

Average **~$9,850 per day**. Best days: 14 Sep ($13,135) and 3 Sep ($13,112). Returns are included as negative amounts on their order's date.

### Revenue by category

```mermaid
pie showData
    title Revenue by category (USD)
    "Electronics" : 223943.51
    "Fashion" : 20869.65
    "Sports" : 18413.67
    "Home" : 15924.47
    "Beauty" : 9639.45
    "Books" : 3834.98
    "UNKNOWN product" : 2959.85
```

| Category | Revenue (USD) | Share |
|---|---:|---:|
| Electronics | 223,943.51 | 75.8% |
| Fashion | 20,869.65 | 7.1% |
| Sports | 18,413.67 | 6.2% |
| Home | 15,924.47 | 5.4% |
| Beauty | 9,639.45 | 3.3% |
| Books | 3,834.98 | 1.3% |
| UNKNOWN (product not in catalogue) | 2,959.85 | 1.0% |
| **Total** | **295,585.58** | **100%** |

### Revenue by customer country (at the time of the order)

```mermaid
xychart-beta horizontal
    title "Revenue by customer country (USD)"
    x-axis ["India", "United States", "United Kingdom", "Germany", "Singapore", "Unknown customer"]
    y-axis "Revenue (USD)" 0 --> 140000
    bar [137095, 62707, 45827, 24157, 20159, 5640]
```

### Top 5 customers

| Rank | Customer | Current tier | Revenue (USD) |
|---:|---|---|---:|
| 1 | C0035 | Silver | 20,337.77 |
| 2 | C0091 | Bronze | 18,499.49 |
| 3 | C0027 | Platinum | 17,524.83 |
| 4 | C0066 | Gold | 17,071.91 |
| 5 | C0058 | Silver | 16,855.55 |

### 🔍 Key takeaways

- **Electronics drives the business.** It is about 76% of revenue, so any pricing or stock issue there moves the whole month.
- **India is the largest market** at 46% of revenue, followed by the US (21%) and the UK (16%).
- **Most orders complete.** Of 945 orders, 666 are delivered and 107 (11%) are cancelled. Cancelled and placed orders are excluded from revenue automatically.
- **14 delivered orders have payment problems.** For example, NC-100090 (₹44,534.76) has no payment, and NC-100279 was **paid exactly double**. Finance should review these.
- **Data issues are visible, not hidden.** 18 orphan lines and 5 orphan payments sit in quarantine, and 14 lines with unknown products are still counted under `UNKNOWN`.

---

## 🧹 Data quality: nothing is dropped silently

Every input row ends up in exactly one place: loaded, quarantined with a reason, or counted as a duplicate or older version.

```mermaid
flowchart LR
    I["2,013 order-item rows<br/>(bronze)"] --> Q1["18 quarantined<br/>ORDER_NOT_FOUND"]
    I --> D1["62 duplicates<br/>later batch wins"]
    I --> OK1["1,933 clean lines<br/>(silver)"]
```

| Entity | Input rows | Quarantined | Duplicates / older | Loaded |
|---|---:|---:|---:|---:|
| Orders (batch 2) | 1,545 | 0 | 948 older or duplicate versions | 597 → MERGE |
| Order items | 2,013 | 18 (order not found) | 62 | 1,933 |
| Customers | 194 | 5 (no `updated_at`) | 4 exact + 19 no-change | 166 versions + 1 unknown |
| Payments | 1,164 | 5 (order not found) | 0 | 1,159 |

**The gate.** The run **fails** if any of these breaks:

| # | Check | Status |
|---|---|:---:|
| 1 | One row per order in `silver.orders` | ✅ |
| 2 | This batch landed in bronze exactly once | ✅ |
| 3 | One fact row per order line | ✅ |
| 4 | Exactly one current version per customer | ✅ |
| 5 | Daily revenue total = category revenue total | ✅ |

---

## 🛡️ Reliability evidence

| Capability | How it's shown | Result |
|---|---|---|
| **Change history** | Transaction log on `silver.orders` | MERGE 495/0 → 450/137 → **0/0 on rerun** |
| **Time travel** | Read the table as of an earlier version | 495 orders then, 945 now. NC-100280 was *paid*, now *delivered* |
| **Schema enforcement** | Append a row with an extra column | ❌ Rejected, row count unchanged |
| **Late data protection** | NC-100039: older *paid* version sent after *cancelled* | Stays **cancelled** |
| **Change data feed** | Row-level changes of the batch-2 MERGE | 450 inserts + 137 before/after updates |
| **Point-in-time customers** | C0004 moved India → Germany on 14 Sep | 12 Sep order → India; later orders → Germany |
| **FX fallback** | NC-100133 ordered Saturday 5 Sep | Uses Friday 4 Sep rate (434 lines used an earlier rate) |
| **Retries + failure path** | Run `batch_9` (file doesn't exist) | 3 attempts → downstream skipped → `on_failure` logged |
| **Idempotency** | Run batch_2 twice | Every table identical |

---

## 🚀 Quick start

```bash
git clone https://github.com/raj3119/novacart-order-analytics.git
cd novacart-order-analytics
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python tests/test_pipeline.py     # 7 acceptance tests
streamlit run app.py              # opens http://localhost:8501
```

On first start the app loads batch_1 → batch_2 → batch_2 automatically.

**Terminal alternative**

```bash
python run_pipeline.py --demo                    # reset + batch_1 + batch_2 + batch_2 again
python run_pipeline.py --batch batch_9           # failure demo: retries, then on_failure
python watch.py                                  # file-arrival trigger: runs when a new batch lands
```

### The Streamlit app

| Page | What you can do |
|---|---|
| **Overview** | Problem, architecture and headline numbers |
| **Run pipeline** | Run any batch, watch the live log, see every task attempt |
| **Gold dashboard** | Daily and category revenue, top customers, payment mismatches, with filters |
| **Data quality** | DQ report, gate status, reconciliation, quarantine tables |
| **Evidence (Delta)** | History, time travel, schema enforcement, OPTIMIZE, late version, change feed, SCD2, FX |
| **Data explorer** | Browse and download any table in any layer |
| **Handbook check** | Expected vs actual for every key number |

### Deploy (Streamlit Community Cloud, free)

1. Push this repo to GitHub.
2. Go to [share.streamlit.io](https://share.streamlit.io) → **Create app** → choose the repo, branch `main`, file `app.py` → **Deploy**.

---

## 📁 Project structure

```
novacart-order-analytics/
├── app.py                  Streamlit UI (7 pages)
├── run_pipeline.py         Command-line runner (--batch, --reset, --demo)
├── watch.py                File-arrival trigger
├── requirements.txt
├── data/input/             8 source files (never modified)
├── pipeline/
│   ├── config.py           Settings: input path, rules, retries
│   ├── lakehouse.py        Storage layer: atomic commits, history, time travel,
│   │                       MERGE, replace-per-batch, schema enforcement, change feed
│   ├── common.py           Timestamp parsing, safe casts, quarantine split, DQ report
│   ├── bronze.py           01 Land files as text + lineage
│   ├── silver_orders.py    02 Clean, quarantine, latest per order, MERGE
│   ├── silver_others.py    03 Items, payments, FX, products, customer SCD2
│   ├── gold.py             04 Fact table + 4 reports
│   ├── dq_checks.py        05 Five-check quality gate
│   ├── job.py              Orchestration: tasks, retries, on_failure, run lock
│   └── evidence.py         06 Reliability evidence
└── tests/test_pipeline.py  End-to-end acceptance tests
```

---

## 🧭 Design decisions and assumptions

| Decision | Why |
|---|---|
| Bronze stores **every value as text** | Nothing is lost when a number arrives as `"50.89"`; silver casts it safely |
| **`updated_at` decides**, never file order | Batch 2 contains older versions that must not overwrite newer ones |
| Strict **`>`** in the MERGE (not `>=`) | A rerun changes nothing and history shows no fake updates |
| Missing currency is **derived from shipping country**; a present currency is never overridden | Follows the brief; 48 orders end with a derived currency |
| `dd/MM/yyyy HH:mm` timestamps are **UTC** | Stated in the brief |
| FX: rate on the order date, **else the most recent earlier rate**; USD = 1 | Covers weekends and the missing Monday 7 Sep |
| New customer version **only when tier or country changes** | Name or email edits don't create history (19 rows skipped) |
| Customers missing from the CRM get **key −1 / UNKNOWN** | Their 39 lines still count |
| Payment mismatches use **revenue orders only** (14 rows) | A literal reading lists every cancelled and placed order (165 rows of noise). One setting in `config.py` switches it |

### How this maps to Databricks / Delta Lake

This edition runs locally on **pandas + SQLite**. The storage layer reproduces the Delta Lake behaviours the pipeline relies on. The same logic runs on **Azure Databricks with PySpark and native Delta** in the cloud version.

| Databricks + Delta | This project |
|---|---|
| Unity Catalog schemas | One SQLite file per layer, attached as `bronze`, `silver`, … |
| Delta commit + `DESCRIBE HISTORY` | Atomic transaction + `_delta_log` row with metrics |
| `VERSION AS OF` | Snapshot per version of `silver.orders` |
| `MERGE INTO … WHEN MATCHED AND …` | `Lake.merge()` with the same rule and metrics |
| `replaceWhere` | `Lake.replace_where()` |
| Schema enforcement | `Lake.append()` rejects unexpected columns |
| Change Data Feed | Insert + update pre/post images per MERGE |
| `OPTIMIZE … ZORDER BY` | Table rewritten sorted by `order_date` |
| Lakeflow Job (parameter, retries, on_failure) | `pipeline/job.py` |
| File-arrival trigger | `watch.py` |

---

<div align="center">

Built by **Team DATAVERSE** for the HCLTech Cloud Data Engineer (AI-Ready) hackathon, October 2026.

</div>
