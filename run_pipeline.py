"""
Run the NovaCart pipeline from the terminal (same thing the Streamlit "Run" buttons do).

  python run_pipeline.py --reset                 # clean start (drops lake/, never touches data/input)
  python run_pipeline.py --batch batch_1
  python run_pipeline.py --batch batch_2
  python run_pipeline.py --batch batch_2         # rerun: nothing changes
  python run_pipeline.py --batch batch_9         # failure demo: retries, then on_failure
  python run_pipeline.py --demo                  # reset + batch_1 + batch_2 + batch_2 again
"""
import argparse
import sys

from pipeline.job import run_job
from pipeline.lakehouse import reset_lake

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", help="batch_1, batch_2, ...")
    ap.add_argument("--reset", action="store_true", help="drop all layers first")
    ap.add_argument("--demo", action="store_true", help="reset, batch_1, batch_2, batch_2 again")
    ap.add_argument("--retry-wait", type=int, default=None, help="seconds between retries")
    a = ap.parse_args()

    if a.reset or a.demo:
        reset_lake()
        print("lake reset (input files untouched)")
    batches = ["batch_1", "batch_2", "batch_2"] if a.demo else ([a.batch] if a.batch else [])
    status = 0
    for b in batches:
        r = run_job(b, retry_wait=a.retry_wait)
        status = status or (r["status"] != "SUCCESS")
    sys.exit(status)
