"""The local catalog: harvested BigQuery metadata in DuckDB, with snapshots.

Reads here never call BigQuery. Writes come from `harvest` in bq.py.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

# Where the catalog lives. Regenerable, so it sits outside any repo by default.
CATALOG = Path(os.environ.get("SOUNDING_CATALOG", Path.home() / ".sounding" / "catalog.duckdb"))
# On-demand list price (US multi-region), used only to put $ next to bytes.
USD_PER_TIB = 6.25
TIB = 1024**4

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id INTEGER, taken_at TIMESTAMP, dataset VARCHAR);
CREATE TABLE IF NOT EXISTS tables (
  snapshot_id INTEGER, dataset VARCHAR, table_id VARCHAR, fqn VARCHAR, type VARCHAR,
  num_rows BIGINT, num_bytes BIGINT, created TIMESTAMP, modified TIMESTAMP,
  partitioning VARCHAR, clustering VARCHAR, description VARCHAR, view_sql VARCHAR);
CREATE TABLE IF NOT EXISTS columns (
  snapshot_id INTEGER, fqn VARCHAR, path VARCHAR, type VARCHAR, mode VARCHAR,
  description VARCHAR, scan_bytes BIGINT);
"""

# The newest snapshot of each dataset.
LATEST = "snapshot_id IN (SELECT max(snapshot_id) FROM snapshots GROUP BY dataset)"


def connect(path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(path or CATALOG)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def usd(num_bytes: int | None) -> float:
    return round((num_bytes or 0) / TIB * USD_PER_TIB, 6)


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def search(con: duckdb.DuckDBPyConnection, q: str = "", limit: int | None = None) -> list[dict]:
    """Tables whose name, column paths or description match `q` (all tables if empty),
    largest first, with the column paths that matched."""
    q = q.strip()
    rows = con.execute(f"""
        SELECT t.fqn, any_value(t.type), any_value(t.num_rows), any_value(t.num_bytes),
               any_value(t.modified), any_value(t.description),
               list(DISTINCT c.path ORDER BY c.path) FILTER (WHERE $q <> '' AND c.path ILIKE '%' || $q || '%')
        FROM tables t LEFT JOIN columns c USING (snapshot_id, fqn)
        WHERE t.{LATEST} AND ($q = '' OR t.fqn ILIKE '%' || $q || '%' OR c.path ILIKE '%' || $q || '%'
                                  OR t.description ILIKE '%' || $q || '%')
        GROUP BY t.fqn
        ORDER BY any_value(t.num_bytes) DESC NULLS LAST, t.fqn
        LIMIT $limit""", {"q": q, "limit": limit or 1_000_000}).fetchall()
    return [{"table": r[0], "type": r[1], "rows": r[2], "bytes": r[3],
             "full_scan_usd": round(usd(r[3]), 4), "modified": _iso(r[4]), "description": r[5],
             "matched_columns": r[6] or [], "source": "catalog (free)"} for r in rows]


def stats(con: duckdb.DuckDBPyConnection) -> dict:
    r = con.execute(f"""
        SELECT count(DISTINCT dataset), count(*), coalesce(sum(num_bytes), 0), max(s.taken_at)
        FROM tables JOIN snapshots s USING (snapshot_id, dataset) WHERE {LATEST}""").fetchone()
    return {"datasets": r[0], "tables": r[1], "bytes": r[2], "harvested_at": _iso(r[3])}


def describe(con: duckdb.DuckDBPyConnection, fqn: str) -> dict:
    cur = con.execute(f"SELECT * FROM tables WHERE {LATEST} AND fqn = ?", [fqn])
    t = cur.fetchone()
    if not t:
        return {"error": f"{fqn} not in catalog; harvest its dataset first"}
    out = {k: _iso(v) for k, v in zip([d[0] for d in cur.description], t)}
    cols = con.execute(
        "SELECT path, type, mode, description, scan_bytes FROM columns WHERE snapshot_id = ? AND fqn = ?",
        [t[0], fqn]).fetchall()
    out["columns"] = [dict(zip(["path", "type", "mode", "description", "scan_bytes"], c)) for c in cols]
    out["source"] = "catalog (tables.get, free)"
    return out
