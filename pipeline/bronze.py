"""01 BRONZE: land every file exactly as received (all text) + lineage. Rerun-safe."""
import json

import pandas as pd

from . import config
from .common import now


def _lineage(df, path, batch_id):
    df = df.copy()
    df["_source_file"] = f"file:{path}"
    df["_batch_id"] = batch_id
    df["_ingested_at"] = now()
    return df


def _read_csv_as_text(path):
    # dtype=str: no type guessing; empty cells become NULL (like Spark's CSV reader)
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def _as_text(v):
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v)            # nested `attributes` kept as JSON text
    return str(v)


def _read_json_as_text(path, multiline=False, fields=None):
    if multiline:
        records = json.loads(path.read_text())
    else:
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    rows = [{f: _as_text(r.get(f)) for f in fields} for r in records]
    return pd.DataFrame(rows, columns=fields)


ITEM_FIELDS = ["order_id", "line_no", "product_id", "qty", "unit_price", "discount_pct", "line_type", "attributes"]
PAY_FIELDS = ["payment_id", "order_id", "method", "amount", "currency", "status", "paid_at", "gateway_ref"]


def run(lake, batch_id, dq, log=print):
    of, itf = config.orders_file(batch_id), config.items_file(batch_id)
    for f in (of, itf):
        if not f.exists():
            raise FileNotFoundError(f"Input file not found: {f.name} (batch '{batch_id}' has not arrived)")

    # batch files: replace only this batch's rows (replaceWhere) -> running a batch twice changes nothing
    orders = _lineage(_read_csv_as_text(of), of, batch_id)
    lake.replace_where("bronze.orders", orders, "_batch_id", batch_id)
    items = _lineage(_read_json_as_text(itf, fields=ITEM_FIELDS), itf, batch_id)
    lake.replace_where("bronze.order_items", items, "_batch_id", batch_id)

    # snapshot files: full overwrite
    lake.overwrite("bronze.customers", _lineage(_read_csv_as_text(config.CUSTOMERS_FILE), config.CUSTOMERS_FILE, batch_id))
    lake.overwrite("bronze.products", _lineage(_read_csv_as_text(config.PRODUCTS_FILE), config.PRODUCTS_FILE, batch_id))
    lake.overwrite("bronze.fx_rates", _lineage(_read_csv_as_text(config.FX_FILE), config.FX_FILE, batch_id))
    lake.overwrite("bronze.payments", _lineage(
        _read_json_as_text(config.PAYMENTS_FILE, multiline=True, fields=PAY_FIELDS), config.PAYMENTS_FILE, batch_id))

    for t, label in [("bronze.orders", "ORDERS_THIS_BATCH"), ("bronze.order_items", "ITEMS_THIS_BATCH")]:
        n = lake.scalar(f"SELECT count(*) FROM {t} WHERE _batch_id = ?", (batch_id,))
        dq.add("bronze", f"LANDED_{label}", n, n)
    for t in ["customers", "products", "fx_rates", "payments"]:
        n = lake.scalar(f"SELECT count(*) FROM bronze.{t}")
        dq.add("bronze", f"LANDED_{t.upper()}_SNAPSHOT", n, n)
    dq.flush("bronze")
    log(f"bronze.orders total rows = {lake.scalar('SELECT count(*) FROM bronze.orders')}, "
        f"bronze.order_items total rows = {lake.scalar('SELECT count(*) FROM bronze.order_items')}")
