"""Local metadata snapshots; no BigQuery calls.

An advisory sidecar lock coordinates all Sounding processes. Connections are short
and never held across network calls. Readers open DuckDB read-only after initialization.
"""

from __future__ import annotations

import datetime as dt
import os
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa
from filelock import FileLock, Timeout

from .errors import CatalogBusy, CatalogVersionError, MissingTable, integer, text

CATALOG = Path(os.environ.get("SOUNDING_CATALOG", Path.home() / ".sounding" / "catalog.duckdb"))
LOCK_TIMEOUT = 10
SCHEMA_VERSION = 2
USD_PER_TIB = 6.25  # US on-demand list price; excludes discounts and the monthly free tier.
TIB = 1024**4
MIB = 1024**2
SEARCH_LIMIT = 100
MATCH_LIMIT = 20

SCHEMA = """
CREATE TABLE catalog_meta (version INTEGER);
INSERT INTO catalog_meta VALUES (2);
CREATE TABLE snapshots (
  snapshot_id BIGINT PRIMARY KEY, taken_at TIMESTAMPTZ NOT NULL, dataset VARCHAR NOT NULL);
CREATE TABLE tables (
  snapshot_id BIGINT, dataset VARCHAR, table_id VARCHAR, fqn VARCHAR, type VARCHAR,
  num_rows BIGINT, num_bytes BIGINT, created TIMESTAMPTZ, modified TIMESTAMPTZ,
  partitioning VARCHAR, clustering VARCHAR, description VARCHAR, view_sql VARCHAR,
  PRIMARY KEY (snapshot_id, fqn));
CREATE TABLE columns (
  snapshot_id BIGINT, fqn VARCHAR, path VARCHAR, type VARCHAR, mode VARCHAR,
  description VARCHAR, scan_bytes BIGINT, PRIMARY KEY (snapshot_id, fqn, path));
"""

LATEST = """WITH latest AS (
    SELECT dataset, max(snapshot_id) AS snapshot_id FROM snapshots GROUP BY dataset
), current_tables AS (
    SELECT t.* FROM tables t JOIN latest l USING (dataset, snapshot_id)
)"""

TABLE_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("dataset", pa.string()), ("table_id", pa.string()),
    ("fqn", pa.string()), ("type", pa.string()), ("num_rows", pa.int64()),
    ("num_bytes", pa.int64()), ("created", pa.timestamp("us", tz="UTC")),
    ("modified", pa.timestamp("us", tz="UTC")), ("partitioning", pa.string()),
    ("clustering", pa.string()), ("description", pa.string()), ("view_sql", pa.string()),
])
COLUMN_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("fqn", pa.string()), ("path", pa.string()),
    ("type", pa.string()), ("mode", pa.string()), ("description", pa.string()),
    ("scan_bytes", pa.int64()),
])


def _check_version(con):
    exists = con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'catalog_meta'").fetchone()[0]
    if not exists or con.execute("SELECT version FROM catalog_meta").fetchall() != [(SCHEMA_VERSION,)]:
        raise CatalogVersionError(
            "This catalog uses an older or unsupported schema. Preserve it and harvest into a new "
            "file using SOUNDING_CATALOG; see README 'Catalog upgrades'. No data was changed.")


@contextmanager
def connect(path: Path | str | None = None, *, write: bool = False):
    """Acquire the process/thread lock before opening DuckDB; always close before unlocking."""
    path = Path(path or CATALOG).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(path) + ".lock", timeout=LOCK_TIMEOUT):
            if not path.exists():
                with duckdb.connect(str(path)) as initial:
                    initial.execute(SCHEMA)
            try:
                con = duckdb.connect(str(path), read_only=not write)
            except duckdb.IOException as error:
                raise CatalogBusy("Catalog is in use by another program. Close it and retry.") from error
            try:
                con.execute("SET TimeZone = 'UTC'")
                _check_version(con)
                yield con
            finally:
                con.close()
    except Timeout as error:
        raise CatalogBusy("Catalog is busy. Retry after the current local operation finishes.") from error


def save_snapshot(dataset: str, table_rows: list[dict], col_rows: list[dict]) -> int:
    """Publish one complete dataset atomically. Failed inserts leave the previous snapshot current."""
    with connect(write=True) as con:
        con.execute("BEGIN")
        try:
            sid = con.execute("SELECT coalesce(max(snapshot_id), 0) + 1 FROM snapshots").fetchone()[0]
            con.execute("INSERT INTO snapshots VALUES (?, ?, ?)", [sid, dt.datetime.now(dt.UTC), dataset])
            for name, rows, schema in [("tables", table_rows, TABLE_SCHEMA), ("columns", col_rows, COLUMN_SCHEMA)]:
                if rows:
                    batch = pa.Table.from_pylist([dict(row, snapshot_id=sid) for row in rows], schema=schema)
                    con.register("batch", batch)
                    # Explicit Arrow fields and BY NAME avoid depending on dict/DDL column ordering.
                    con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM batch")
                    con.unregister("batch")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return sid


def usd(num_bytes: int | None) -> float | None:
    return None if num_bytes is None else round(num_bytes / TIB * USD_PER_TIB, 8)


def billing_range(num_bytes: int | None, table_count: int = 1) -> dict | None:
    """Scan-price range including each table's minimum, given only aggregate bytes.

    For multiple tables we cannot reconstruct their individual billable bytes.
    Distributing the bytes evenly vs. concentrating them gives these bounds.
    """
    if num_bytes is None or num_bytes <= 0 or table_count < 1:
        return None
    rounded = ((num_bytes + MIB - 1) // MIB) * MIB
    lower = max(rounded, table_count * 10 * MIB)
    upper = max(rounded, 10 * MIB) + (table_count - 1) * 10 * MIB
    return {"min": usd(lower), "max": usd(upper)}


def full_scan_usd(kind, num_bytes):
    cost = billing_range(num_bytes) if kind == "TABLE" else None
    return cost["max"] if cost else None


def _iso(value):
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC).isoformat()
    return value


def search(con, q: str = "", limit: int = 50, offset: int = 0) -> dict:
    """A bounded page of literal substring matches and a bounded sample of matching paths."""
    q = text(q, "q", empty=True).strip()
    integer(limit, "limit", 1, SEARCH_LIMIT)
    integer(offset, "offset", 0, 1_000_000)
    # contains() treats %, _, and backslashes literally; parameters protect SQL syntax.
    rows = con.execute(LATEST + """, matched AS (
        SELECT c.snapshot_id, c.fqn, c.path,
               row_number() OVER (PARTITION BY c.snapshot_id, c.fqn ORDER BY c.path) AS rn,
               count(*) OVER (PARTITION BY c.snapshot_id, c.fqn) AS total
        FROM columns c JOIN current_tables t USING (snapshot_id, fqn)
        WHERE $q <> '' AND contains(lower(c.path), lower($q))
    ), matches AS (
        SELECT snapshot_id, fqn, list(path ORDER BY path) AS paths, max(total) AS total
        FROM matched WHERE rn <= $match_limit GROUP BY snapshot_id, fqn
    )
    SELECT t.fqn, t.type, t.num_rows, t.num_bytes, t.modified,
           substr(t.description, 1, 1000), m.paths, coalesce(m.total, 0)
    FROM current_tables t LEFT JOIN matches m USING (snapshot_id, fqn)
    WHERE $q = '' OR contains(lower(t.fqn), lower($q))
        OR contains(lower(t.description), lower($q)) OR m.total > 0
    ORDER BY t.num_bytes DESC NULLS LAST, t.fqn
    LIMIT $limit OFFSET $offset
    """, {"q": q, "match_limit": MATCH_LIMIT, "limit": limit + 1, "offset": offset}).fetchall()
    return {"items": [
        {"table": r[0], "type": r[1], "rows": r[2], "bytes": r[3],
         "full_scan_usd": full_scan_usd(r[1], r[3]), "modified": _iso(r[4]), "description": r[5],
         "matched_columns": r[6] or [], "matched_column_count": r[7], "source": "catalog (free)"}
        for r in rows[:limit]],
        "next_offset": offset + limit if len(rows) > limit else None}


def stats(con) -> dict:
    r = con.execute(LATEST + """
        SELECT (SELECT count(*) FROM latest), count(*), coalesce(sum(num_bytes), 0),
               (SELECT max(taken_at) FROM snapshots JOIN latest USING (dataset, snapshot_id))
        FROM current_tables
    """).fetchone()
    return {"datasets": r[0], "tables": r[1], "bytes": r[2], "harvested_at": _iso(r[3])}


def describe(con, fqn: str) -> dict:
    text(fqn, "table")
    cur = con.execute(LATEST + " SELECT * FROM current_tables WHERE fqn = ?", [fqn])
    row = cur.fetchone()
    if row is None:
        raise MissingTable("Table is not in the latest catalog snapshot; harvest its dataset first.")
    out = {k: _iso(v) for k, v in zip([d[0] for d in cur.description], row)}
    cols = con.execute(
        "SELECT path, type, mode, description, scan_bytes FROM columns WHERE snapshot_id = ? AND fqn = ? ORDER BY rowid",
        [out["snapshot_id"], fqn]).fetchall()
    out["columns"] = [dict(zip(["path", "type", "mode", "description", "scan_bytes"], c)) for c in cols]
    out["full_scan_usd"] = full_scan_usd(out["type"], out["num_bytes"])
    out["source"] = "catalog (tables.get, free)"
    return out
