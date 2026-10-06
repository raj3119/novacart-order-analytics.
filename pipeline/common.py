"""Helpers shared by all steps: timestamp parsing, safe casts, quarantine split, DQ report."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pandas as pd

from . import config


def isnull(v):
    """True for None, NaN and pandas NA (pandas may store None as NaN)."""
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def blank_to_none(v):
    if isnull(v):
        return None
    s = str(v).strip()
    return s if s else None


def parse_ts(v):
    """Try each known format; return 'YYYY-MM-DD HH:MM:SS' in UTC, or None (never raises)."""
    s = blank_to_none(v)
    if s is None:
        return None
    s2 = s[:-1] + "+00:00" if s.endswith("Z") else s
    for fmt in config.TS_FORMATS:
        try:
            d = datetime.strptime(s2, fmt)
        except ValueError:
            continue
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc).replace(tzinfo=None)
        return d.strftime("%Y-%m-%d %H:%M:%S")
    return None


def try_int(v):
    s = blank_to_none(v)
    if s is None:
        return None
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    return int(d) if d == d.to_integral_value() else None


def try_dec(v, places):
    """try_cast(v AS DECIMAL(p, places)): None when not a number; rounded half-up like Spark."""
    s = blank_to_none(v)
    if s is None:
        return None
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    if not d.is_finite():
        return None
    return d.quantize(Decimal(1).scaleb(-places), rounding="ROUND_HALF_UP")


def first_failing_rule(row, rules):
    for check, name in rules:
        if check(row):
            return name
    return None


def split(df, rules, stage, dq, quarantine_cols):
    """Tag each row with the first failing rule; return (good, quarantine_df) and log one DQ row per rule."""
    reasons = df.apply(lambda r: first_failing_rule(r, rules), axis=1) if len(df) else pd.Series([], dtype=object)
    df = df.assign(_reject_reason=reasons)
    bad = df[df["_reject_reason"].notna()]
    remaining = len(df)
    for _, rule in rules:
        n = int((bad["_reject_reason"] == rule).sum())
        dq.add(stage, rule, remaining, remaining - n, quarantined=n)
        remaining -= n
    q = bad[quarantine_cols + ["_reject_reason"]].copy()
    q["_quarantined_at"] = now()
    return df[df["_reject_reason"].isna()].drop(columns=["_reject_reason"]), q


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class DQ:
    """Collects data-quality rows; flush() replaces this batch + stage (reruns never double count)."""
    COLS = ["run_ts", "batch_id", "stage", "rule", "rows_in", "rows_out", "rows_quarantined", "rows_flagged"]

    def __init__(self, lake, batch_id, log=print):
        self.lake, self.batch_id, self.rows, self.log = lake, batch_id, [], log
        self.run_ts = now()

    def add(self, stage, rule, rows_in, rows_out, quarantined=0, flagged=0):
        self.rows.append([self.run_ts, self.batch_id, stage, rule, int(rows_in), int(rows_out),
                          int(quarantined), int(flagged)])
        self.log(f"[DQ] {stage:<13} {rule:<38} in={rows_in:>5} out={rows_out:>5} "
                 f"quarantined={quarantined:>4} flagged={flagged:>4}")

    def flush(self, stage):
        df = pd.DataFrame([r for r in self.rows if r[2] == stage], columns=self.COLS)
        lake = self.lake
        lake.create_if_not_exists("dq.dq_report", pd.DataFrame(
            [["", "", "", "", 0, 0, 0, 0]], columns=self.COLS))
        cur = lake.conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM dq.dq_report WHERE batch_id = ? AND stage = ?", (self.batch_id, stage))
        lake._insert(cur, "dq.dq_report", df)
        lake._commit_log(cur, "dq.dq_report", "WRITE",
                         {"replaceWhere": f"batch_id = '{self.batch_id}' AND stage = '{stage}'"},
                         {"numOutputRows": len(df)})
        lake.conn.commit()
