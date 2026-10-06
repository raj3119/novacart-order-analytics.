"""Shared settings. INPUT_PATH is the only line you normally edit."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ---- the only line to edit -------------------------------------------------
INPUT_PATH = ROOT / "data" / "input"
# ----------------------------------------------------------------------------

LAKE_PATH = ROOT / "lake"                      # one SQLite file per layer (= Delta schemas)
SCHEMAS = ["bronze", "silver", "gold", "quarantine", "dq"]

VALID_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
REVENUE_STATUSES = ["paid", "shipped", "delivered"]
VALID_CURRENCIES = ["INR", "USD", "GBP", "EUR", "SGD"]
COUNTRY_CURRENCY = {"IN": "INR", "US": "USD", "GB": "GBP", "DE": "EUR", "SG": "SGD"}
PAY_STATUSES = ["success", "failed", "refunded"]

MISMATCH_TOLERANCE = 0.50
MISMATCH_REVENUE_ORDERS_ONLY = True     # assumption; False = literal reading of the PDF (165 rows)

# orchestration (Databricks job uses 2 retries, 60 s apart; local default is shorter for the demo)
RETRIES = 2
RETRY_WAIT_SECONDS = 3

TS_FORMATS = ["%Y-%m-%dT%H:%M:%S%z",      # 2026-09-11T14:36:07Z and +05:30
              "%Y-%m-%dT%H:%M:%S.%f%z",
              "%Y-%m-%dT%H:%M:%S",        # ISO without zone -> UTC
              "%d/%m/%Y %H:%M",           # read as UTC (PDF rule)
              "%Y-%m-%d"]                 # date only -> midnight UTC

LOW_DATE = "1900-01-01 00:00:00"
HIGH_DATE = "9999-12-31 00:00:00"


def validate_batch(batch_id: str) -> str:
    batch_id = (batch_id or "").strip()
    if not re.fullmatch(r"batch_\d+", batch_id):
        raise ValueError(f"batch_id must look like batch_<number>, got '{batch_id}'")
    return batch_id


def batch_no(batch_id: str) -> int:
    return int(batch_id.split("_")[1])


def orders_file(batch_id):
    return INPUT_PATH / f"orders_{batch_id}.csv"


def items_file(batch_id):
    return INPUT_PATH / f"order_items_{batch_id}.jsonl"


CUSTOMERS_FILE = INPUT_PATH / "customers_changes.csv"
PRODUCTS_FILE = INPUT_PATH / "products.csv"
FX_FILE = INPUT_PATH / "fx_rates.csv"
PAYMENTS_FILE = INPUT_PATH / "payments.json"
