"""
Orchestration = the Lakeflow Job, locally.
  bronze -> silver_orders -> silver_others -> gold -> dq_checks   (each: 2 retries)
  on_failure runs only if at least one task failed (writes dq.pipeline_failures)
  one run at a time (max_concurrent_runs = 1) via a lock file
Every task attempt is logged to dq.job_runs (the "run page").
"""
import os
import time
import traceback
import uuid

import pandas as pd

from . import bronze, config, dq_checks, gold, silver_orders, silver_others
from .common import DQ, now
from .lakehouse import Lake

TASKS = [("bronze", bronze.run), ("silver_orders", silver_orders.run), ("silver_others", silver_others.run),
         ("gold", gold.run), ("dq_checks", dq_checks.run)]
RUN_COLS = ["job_run_id", "batch_id", "task", "attempt", "status", "started_at", "ended_at", "seconds", "error"]


class ConcurrentRunError(Exception):
    pass


def _record(lake, row):
    lake.create_if_not_exists("dq.job_runs", pd.DataFrame([["", "", "", 0, "", "", "", 0.0, ""]], columns=RUN_COLS))
    cur = lake.conn.cursor()
    cur.execute("BEGIN")
    lake._insert(cur, "dq.job_runs", pd.DataFrame([row], columns=RUN_COLS))
    lake.conn.commit()


def _on_failure(lake, batch_id, run_id, message):
    lake.create_if_not_exists("dq.pipeline_failures", pd.DataFrame(
        [["", "", "", ""]], columns=["failed_at", "batch_id", "job_run_id", "message"]))
    cur = lake.conn.cursor()
    cur.execute("BEGIN")
    lake._insert(cur, "dq.pipeline_failures", pd.DataFrame(
        [[now(), batch_id, run_id, message]], columns=["failed_at", "batch_id", "job_run_id", "message"]))
    lake.conn.commit()


def run_job(batch_id, log=print, retries=None, retry_wait=None):
    retries = config.RETRIES if retries is None else retries
    retry_wait = config.RETRY_WAIT_SECONDS if retry_wait is None else retry_wait
    config.LAKE_PATH.mkdir(parents=True, exist_ok=True)
    lock = config.LAKE_PATH / ".job.lock"
    if lock.exists() and time.time() - lock.stat().st_mtime < 3600:
        raise ConcurrentRunError("Another run is in progress (max_concurrent_runs = 1)")
    lock.write_text(str(os.getpid()))

    run_id = "run-" + uuid.uuid4().hex[:8]
    lake = Lake()
    result = {"job_run_id": run_id, "batch_id": batch_id, "status": "SUCCESS", "tasks": []}
    log(f"=== Job run {run_id}  batch_id={batch_id} ===")
    try:
        failed_task = None
        for name, fn in TASKS:
            if failed_task:
                _record(lake, [run_id, batch_id, name, 0, "SKIPPED", now(), now(), 0.0, f"upstream {failed_task} failed"])
                result["tasks"].append((name, "SKIPPED", 0))
                log(f"--- {name}: SKIPPED (upstream failed)")
                continue
            for attempt in range(1, retries + 2):
                t0, started = time.time(), now()
                log(f"--- {name}: attempt {attempt}")
                try:
                    config.validate_batch(batch_id)
                    dq = DQ(lake, batch_id, log)
                    fn(lake, batch_id, dq, log)
                    secs = round(time.time() - t0, 2)
                    _record(lake, [run_id, batch_id, name, attempt, "SUCCESS", started, now(), secs, ""])
                    result["tasks"].append((name, "SUCCESS", attempt))
                    log(f"--- {name}: SUCCESS in {secs}s")
                    break
                except Exception as e:
                    secs = round(time.time() - t0, 2)
                    msg = f"{type(e).__name__}: {e}"
                    _record(lake, [run_id, batch_id, name, attempt, "FAILED", started, now(), secs, msg[:500]])
                    log(f"--- {name}: FAILED attempt {attempt}: {msg}")
                    if attempt <= retries:
                        log(f"    retrying in {retry_wait}s ...")
                        time.sleep(retry_wait)
                    else:
                        failed_task = name
                        result["tasks"].append((name, "FAILED", attempt))
                        result["status"] = "FAILED"
                        result["error"] = msg
        if failed_task:
            log("--- on_failure: running (at least one task failed)")
            _on_failure(lake, batch_id, run_id, f"{failed_task} failed: {result.get('error', '')}"[:500])
            _record(lake, [run_id, batch_id, "on_failure", 1, "SUCCESS", now(), now(), 0.0, ""])
        else:
            _record(lake, [run_id, batch_id, "on_failure", 0, "EXCLUDED", now(), now(), 0.0, "all tasks succeeded"])
        log(f"=== Job run {run_id} finished: {result['status']} ===")
        return result
    except Exception:
        log(traceback.format_exc())
        raise
    finally:
        lake.close()
        lock.unlink(missing_ok=True)
