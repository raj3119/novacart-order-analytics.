"""
File-arrival trigger (local version of the Databricks file-arrival trigger).
Watches data/input/ and starts a job run when a new orders_batch_<N>.csv lands.

  python watch.py            # Ctrl+C to stop
"""
import re
import time

from pipeline import config
from pipeline.job import run_job
from pipeline.lakehouse import Lake

POLL_SECONDS = 10


def loaded():
    L = Lake()
    try:
        if not L.exists("bronze.orders"):
            return set()
        return set(L.sql("SELECT DISTINCT _batch_id FROM bronze.orders")["_batch_id"])
    finally:
        L.close()


if __name__ == "__main__":
    print(f"Watching {config.INPUT_PATH} every {POLL_SECONDS}s ...")
    while True:
        done = loaded()
        for f in sorted(config.INPUT_PATH.glob("orders_batch_*.csv")):
            b = re.match(r"orders_(batch_\d+)\.csv", f.name).group(1)
            if b not in done and config.items_file(b).exists():
                print(f"New batch arrived: {b} -> starting job run")
                run_job(b)
        time.sleep(POLL_SECONDS)
