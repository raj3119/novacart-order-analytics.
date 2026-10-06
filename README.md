# NovaCart Order Analytics (local edition + Streamlit UI)

The NovaCart medallion pipeline (bronze → silver → gold) running on your laptop, with a Streamlit UI
to run batches and show results, data quality and the Delta-style evidence to the panel.

**Stack:** Python 3.9+, pandas, SQLite (one database file per layer, used as a small Delta-like lakehouse), Streamlit.

## Run it (macOS, VS Code terminal)
```bash
cd ~/Desktop/novacart_local
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python tests/test_pipeline.py        # 7 acceptance tests against the handbook numbers
streamlit run app.py                 # UI at http://localhost:8501
```
In the UI: **Run pipeline → 🎬 Full demo**, then **💥 Run batch_9** for the failure path.
Terminal alternative: `python run_pipeline.py --demo` and `python run_pipeline.py --batch batch_9`.

## Put it on the cloud (Streamlit Community Cloud, free)
1. Create a GitHub repo and push this folder (`lake/` is ignored; it is rebuilt at runtime).
2. Go to share.streamlit.io → **Create app** → pick the repo, branch `main`, file `app.py` → **Deploy**.
3. Open the app URL and click **Full demo** once (the cloud container starts with an empty lake).

## Project layout
```
novacart_local/
├── app.py                  Streamlit UI (7 pages)
├── run_pipeline.py         terminal runner (--batch, --reset, --demo)
├── watch.py                file-arrival trigger (starts a run when a new batch lands)
├── data/input/             the 8 source files (never modified)
├── pipeline/
│   ├── config.py           INPUT_PATH (only line to edit), rules, retries
│   ├── lakehouse.py        Delta-like storage: atomic commits, history, time travel,
│   │                       MERGE, replaceWhere, schema enforcement, change feed, OPTIMIZE
│   ├── common.py           timestamp parser, safe casts, quarantine split, DQ report
│   ├── bronze.py           01 land as text + lineage
│   ├── silver_orders.py    02 clean, quarantine, latest per order, MERGE
│   ├── silver_others.py    03 products, FX, items, payments, customer SCD2
│   ├── gold.py             04 fact table + 4 reports
│   ├── dq_checks.py        05 five-check gate
│   ├── job.py              orchestration: 5 tasks, 2 retries, on_failure, 1 run at a time
│   └── evidence.py         06 history, time travel, schema, optimize, late version, CDF
└── tests/test_pipeline.py  acceptance tests
```

## Results (identical to the handbook)
| Measure | After batch 1 | After batch 2 | After batch 2 again |
|---|---|---|---|
| bronze.orders rows | 1,418 | 2,963 | 2,963 |
| MERGE inserted / updated | 495 / 0 | 450 / 137 | 0 / 0 |
| silver.orders rows | 495 | 945 | 945 |
| silver.order_items rows | 971 | 1,933 | 1,933 |
| silver.dim_customer rows | 167 | 167 | 167 |
| quarantine items / customers / payments | 10 / 5 / 537 | 18 / 5 / 5 | 18 / 5 / 5 |
| gold.fact_order_line rows | 971 | 1,933 | 1,933 |
| Total revenue USD | 150,062.65 | 295,585.58 | 295,585.58 |
| Payment mismatches | 42 | 14 | 14 |
| Gate checks | 5 pass | 5 pass | 5 pass |

## How the local lakehouse maps to Databricks / Delta
| Databricks + Delta | Here |
|---|---|
| Unity Catalog schemas bronze/silver/gold/quarantine/dq | `lake/bronze.db`, `lake/silver.db`, … attached as schemas (`silver.orders`) |
| Delta commit (atomic) | one SQLite transaction + a row in `_delta_log` |
| `DESCRIBE HISTORY` | `_delta_log` (version, operation, metrics) |
| `VERSION AS OF` | snapshot per version of `silver.orders` |
| `MERGE … WHEN MATCHED AND s.updated_at > t.updated_at` | `Lake.merge()` with the same rule and metrics |
| `replaceWhere _batch_id = 'batch_2'` | `Lake.replace_where()` |
| Schema enforcement | `Lake.append()` rejects unexpected columns |
| Change Data Feed / `table_changes` | insert + update pre/post images per MERGE version |
| `OPTIMIZE … ZORDER BY (order_date)` | table rewritten sorted by order_date |
| Lakeflow Job (parameter, retries, on_failure, max 1 run) | `pipeline/job.py` |
| File-arrival trigger | `watch.py` |

The same logic is in the Databricks notebooks for the cloud version; the local edition exists so the
pipeline can be verified and demoed without cloud cost or setup.
