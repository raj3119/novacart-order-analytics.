"""
A small local lakehouse that behaves like Delta Lake for this project.

* One SQLite file per layer (bronze.db, silver.db, ...) attached under its schema name,
  so tables are addressed exactly like Unity Catalog: `silver.orders`.
* Every write is one transaction (atomic, like a Delta commit) and is recorded in a
  transaction log (`_delta_log`) with a version number, operation and metrics
  -> DESCRIBE HISTORY.
* Versioned tables keep a snapshot per version -> VERSION AS OF (time travel).
* Versioned tables also keep row-level changes -> table_changes (Change Data Feed).
* append() refuses columns that do not match the table -> schema enforcement.
* MERGE with a condition, replaceWhere and OPTIMIZE ZORDER BY are implemented below.
"""
import json
import math
import shutil
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

from . import config

VERSIONED = {"silver.orders"}          # time travel + change data feed enabled


class SchemaMismatchError(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _clean(v):
    """Python value -> SQLite value (NaN/NaT -> NULL, bool -> 0/1)."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if v is pd.NaT:
        return None
    if isinstance(v, bool):
        return int(v)
    if hasattr(v, "item") and not isinstance(v, (str, bytes)):   # numpy scalar
        try:
            v = v.item()
        except Exception:
            pass
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, bool):
            return int(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return v


def _sql_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "INTEGER"
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    sample = series.dropna()
    if len(sample) and all(isinstance(v, (Decimal, float, int)) and not isinstance(v, bool) for v in sample.head(50)):
        return "REAL"                      # Decimal amounts are stored as numbers, not text
    return "TEXT"


class Lake:
    def __init__(self, path=None):
        self.path = path or config.LAKE_PATH
        self.path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path / "_delta_log.db", check_same_thread=False, timeout=30)
        for s in config.SCHEMAS:
            self.conn.execute(f"ATTACH DATABASE '{self.path / (s + '.db')}' AS {s}")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS _delta_log (
            table_name TEXT, version INTEGER, timestamp TEXT, operation TEXT,
            operation_parameters TEXT, operation_metrics TEXT)""")
        self.conn.commit()

    # ---------------------------------------------------------------- reading
    def sql(self, query, params=()):
        return pd.read_sql_query(query, self.conn, params=params)

    def scalar(self, query, params=()):
        return self.conn.execute(query, params).fetchone()[0]

    def exists(self, name):
        schema, table = name.split(".")
        return self.conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None

    def read(self, name, version=None):
        if version is None:
            return self.sql(f"SELECT * FROM {name}")
        if name not in VERSIONED:
            raise ValueError(f"time travel is enabled only for {VERSIONED}")
        schema, table = name.split(".")
        df = self.sql(f"SELECT * FROM {schema}.__v_{table} WHERE _version = ?", (int(version),))
        return df.drop(columns=["_version"])

    def columns(self, name):
        schema, table = name.split(".")
        return [r[1] for r in self.conn.execute(f"PRAGMA {schema}.table_info({table})")]

    def history(self, name):
        df = self.sql("SELECT version, timestamp, operation, operation_parameters, operation_metrics "
                      "FROM _delta_log WHERE table_name = ? ORDER BY version", (name,))
        return df

    def current_version(self, name):
        v = self.scalar("SELECT max(version) FROM _delta_log WHERE table_name = ?", (name,))
        return -1 if v is None else int(v)

    def table_changes(self, name, v_from, v_to):
        schema, table = name.split(".")
        return self.sql(f"SELECT * FROM {schema}.__cdf_{table} WHERE _commit_version BETWEEN ? AND ? "
                        f"ORDER BY _commit_version", (int(v_from), int(v_to)))

    # ---------------------------------------------------------------- writing
    def _create(self, cur, name, df):
        cols = ", ".join(f'"{c}" {_sql_type(df[c])}' for c in df.columns)
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(f"CREATE TABLE {name} ({cols})")

    def _insert(self, cur, name, df):
        if df.empty:
            return
        cols = ", ".join(f'"{c}"' for c in df.columns)
        marks = ", ".join("?" for _ in df.columns)
        rows = [tuple(_clean(v) for v in row) for row in df.itertuples(index=False, name=None)]
        cur.executemany(f"INSERT INTO {name} ({cols}) VALUES ({marks})", rows)

    def _commit_log(self, cur, name, operation, params=None, metrics=None):
        v = cur.execute("SELECT coalesce(max(version), -1) + 1 FROM _delta_log WHERE table_name = ?",
                        (name,)).fetchone()[0]
        cur.execute("INSERT INTO _delta_log VALUES (?, ?, ?, ?, ?, ?)",
                    (name, v, _now(), operation, json.dumps(params or {}), json.dumps(metrics or {})))
        return v

    def _snapshot(self, cur, name, version):
        """Keep a copy of the table as of this version (time travel)."""
        if name not in VERSIONED:
            return
        schema, table = name.split(".")
        cols = ", ".join(f'"{c}"' for c in self.columns(name))
        if not self.exists(f"{schema}.__v_{table}"):
            cur.execute(f"CREATE TABLE {schema}.__v_{table} AS SELECT {version} AS _version, * FROM {name} WHERE 0")
        cur.execute(f"INSERT INTO {schema}.__v_{table} SELECT {version}, {cols} FROM {name}")

    def _cdf(self, cur, name, version, changes: pd.DataFrame):
        if name not in VERSIONED or changes is None or changes.empty:
            return
        schema, table = name.split(".")
        changes = changes.copy()
        changes["_commit_version"] = version
        if not self.exists(f"{schema}.__cdf_{table}"):
            self._create(cur, f"{schema}.__cdf_{table}", changes)
        self._insert(cur, f"{schema}.__cdf_{table}", changes)

    def overwrite(self, name, df, operation="CREATE OR REPLACE TABLE", params=None):
        """Full rebuild in one atomic commit."""
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            self._create(cur, name, df)
            self._insert(cur, name, df)
            v = self._commit_log(cur, name, operation, params, {"numOutputRows": len(df)})
            self._snapshot(cur, name, v)
            self.conn.commit()
            return v
        except Exception:
            self.conn.rollback()
            raise

    def create_if_not_exists(self, name, df_template):
        if not self.exists(name):
            cur = self.conn.cursor()
            cur.execute("BEGIN")
            self._create(cur, name, df_template.iloc[0:0])
            self._commit_log(cur, name, "CREATE TABLE", {"columns": list(df_template.columns)})
            self.conn.commit()

    def replace_where(self, name, df, column, value):
        """Delta `replaceWhere`: delete and rewrite only rows where column = value, atomically."""
        if not self.exists(name):
            return self.overwrite(name, df, "WRITE", {"mode": "Overwrite", "replaceWhere": f"{column} = '{value}'"})
        self._check_schema(name, df)
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            deleted = cur.execute(f'DELETE FROM {name} WHERE "{column}" = ?', (value,)).rowcount
            self._insert(cur, name, df[self.columns(name)])
            v = self._commit_log(cur, name, "WRITE", {"mode": "Overwrite", "replaceWhere": f"{column} = '{value}'"},
                                 {"numDeletedRows": deleted, "numOutputRows": len(df)})
            self._snapshot(cur, name, v)
            self.conn.commit()
            return v
        except Exception:
            self.conn.rollback()
            raise

    def _check_schema(self, name, df):
        expected = self.columns(name)
        if list(df.columns) != expected and set(df.columns) != set(expected):
            extra = sorted(set(df.columns) - set(expected))
            missing = sorted(set(expected) - set(df.columns))
            raise SchemaMismatchError(
                f"A schema mismatch detected when writing to the table {name}. "
                f"Unexpected columns: {extra}; missing columns: {missing}. Write rejected; table unchanged.")

    def append(self, name, df):
        """Append with schema enforcement (Delta refuses unexpected columns)."""
        self._check_schema(name, df)
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            self._insert(cur, name, df[self.columns(name)])
            v = self._commit_log(cur, name, "WRITE", {"mode": "Append"}, {"numOutputRows": len(df)})
            self._snapshot(cur, name, v)
            self.conn.commit()
            return v
        except Exception:
            self.conn.rollback()
            raise

    def merge(self, name, staged: pd.DataFrame, key: str, newer_col: str):
        """
        MERGE INTO name t USING staged s ON t.key = s.key
        WHEN MATCHED AND s.newer_col > t.newer_col THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        self._check_schema(name, staged)
        cols = self.columns(name)
        staged = staged[cols]
        target = self.read(name).set_index(key, drop=False)
        inserts, updates, pre, post = [], [], [], []
        for row in staged.to_dict("records"):
            k = row[key]
            if k not in target.index:
                inserts.append(row)
            else:
                t = target.loc[k]
                if row[newer_col] is not None and t[newer_col] is not None and str(row[newer_col]) > str(t[newer_col]):
                    updates.append(row)
                    pre.append({c: t[c] for c in cols})
                    post.append(row)
        metrics = {"numSourceRows": len(staged), "numTargetRowsInserted": len(inserts),
                   "numTargetRowsUpdated": len(updates),
                   "numTargetRowsLeftUntouched": len(staged) - len(inserts) - len(updates)}
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            for row in updates:
                sets = ", ".join(f'"{c}" = ?' for c in cols if c != key)
                cur.execute(f'UPDATE {name} SET {sets} WHERE "{key}" = ?',
                            tuple(_clean(row[c]) for c in cols if c != key) + (row[key],))
            self._insert(cur, name, pd.DataFrame(inserts, columns=cols))
            v = self._commit_log(cur, name, "MERGE",
                                 {"predicate": f"t.{key} = s.{key}",
                                  "matchedPredicates": f"s.{newer_col} > t.{newer_col} -> update",
                                  "notMatchedPredicates": "insert"}, metrics)
            self._snapshot(cur, name, v)
            changes = pd.concat([
                pd.DataFrame(inserts, columns=cols).assign(_change_type="insert"),
                pd.DataFrame(pre, columns=cols).assign(_change_type="update_preimage"),
                pd.DataFrame(post, columns=cols).assign(_change_type="update_postimage")], ignore_index=True)
            self._cdf(cur, name, v, changes)
            self.conn.commit()
            return metrics
        except Exception:
            self.conn.rollback()
            raise

    def optimize(self, name, zorder_by):
        """OPTIMIZE ... ZORDER BY: rewrite the table physically sorted by the column(s)."""
        df = self.read(name)
        cur = self.conn.cursor()
        cur.execute("BEGIN")
        self._create(cur, name, df)
        self._insert(cur, name, df.sort_values(zorder_by, kind="stable"))
        v = self._commit_log(cur, name, "OPTIMIZE", {"zOrderBy": zorder_by},
                             {"numFilesRemoved": 1, "numFilesAdded": 1, "rowsRewritten": len(df)})
        self.conn.commit()
        return v

    def close(self):
        self.conn.close()


def reset_lake(path=None):
    """Drop all layers (never touches data/input)."""
    path = path or config.LAKE_PATH
    if path.exists():
        shutil.rmtree(path)
