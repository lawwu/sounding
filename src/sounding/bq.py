"""The only module that calls BigQuery. Every call here is on a $0 rung:

  tables.list / tables.get   REST metadata, API quota only
  dry run                    bytes a query would scan; nothing executes
  tabledata.list             preview rows (tables only, not views)

Nothing here runs a billed query, and INFORMATION_SCHEMA is deliberately not used
(it bills a 10 MB minimum per query and is never cached).
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from google.cloud import bigquery

from . import catalog

# Project that dry-run jobs are attributed to. Dry runs are not billed, but BigQuery
# needs a project for them. Unset: the gcloud/ADC default project.
JOB_PROJECT = os.environ.get("SOUNDING_JOB_PROJECT")
# Parallel tables.get calls. Harvest is network-bound, so threads are enough.
GET_TABLE_WORKERS = 16


def client() -> bigquery.Client:
    return bigquery.Client(project=JOB_PROJECT)


def flatten(fields, prefix=""):
    """Yield (path, field) for every field, descending into RECORDs: hits.page.pagePath."""
    for f in fields:
        path = f"{prefix}{f.name}"
        yield path, f
        if f.field_type in ("RECORD", "STRUCT"):
            yield from flatten(f.fields, path + ".")


def _partitioning(t) -> str | None:
    if t.time_partitioning:
        return f"{t.time_partitioning.type_}:{t.time_partitioning.field or '_PARTITIONTIME'}"
    if t.range_partitioning:
        return f"RANGE:{t.range_partitioning.field}"
    return None


def harvest(datasets: list[str], con=None) -> None:
    con = con or catalog.connect()
    c = client()
    for ds in datasets:
        sid = con.execute("SELECT coalesce(max(snapshot_id), 0) + 1 FROM snapshots").fetchone()[0]
        refs = [i.reference for i in c.list_tables(ds)]
        with ThreadPoolExecutor(GET_TABLE_WORKERS) as pool:
            fetched = list(pool.map(c.get_table, refs))
        table_rows, col_rows = [], []
        for t in fetched:
            fqn = f"{t.project}.{t.dataset_id}.{t.table_id}"
            table_rows.append({
                "snapshot_id": sid, "dataset": ds, "table_id": t.table_id, "fqn": fqn,
                "type": t.table_type, "num_rows": t.num_rows, "num_bytes": t.num_bytes,
                "created": t.created, "modified": t.modified, "partitioning": _partitioning(t),
                "clustering": ",".join(t.clustering_fields) if t.clustering_fields else None,
                "description": t.description, "view_sql": t.view_query})
            # Some public tables have an empty schema; they contribute no columns.
            col_rows += [{"snapshot_id": sid, "fqn": fqn, "path": p, "type": f.field_type,
                          "mode": f.mode, "description": f.description} for p, f in flatten(t.schema)]
        # Bulk inserts via Arrow. DuckDB executemany is row-at-a-time: it took 133 s
        # for the 123k nested column paths of google_analytics_sample.
        con.execute("BEGIN")
        con.execute("INSERT INTO snapshots VALUES (?, ?, ?)", [sid, dt.datetime.now(dt.UTC), ds])
        if table_rows:
            tbatch = pa.Table.from_pylist(table_rows)  # noqa: F841 (read by DuckDB by name)
            con.execute("INSERT INTO tables SELECT * FROM tbatch")
        if col_rows:
            cbatch = pa.Table.from_pylist(col_rows)  # noqa: F841
            con.execute("INSERT INTO columns SELECT *, NULL::BIGINT FROM cbatch")
        con.execute("COMMIT")
        print(f"{ds}: {len(fetched)} tables, {len(col_rows)} column paths (snapshot {sid})",
              file=sys.stderr)


def dry_run(sql: str) -> dict:
    job = client().query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    b = job.total_bytes_processed or 0
    return {"bytes": b, "usd_on_demand": catalog.usd(b),
            "referenced_tables": [f"{r.project}.{r.dataset_id}.{r.table_id}" for r in job.referenced_tables],
            "source": "dry run (free)"}


def column_costs(fqn: str, con=None) -> list[tuple[str, int]]:
    """Dry-run `SELECT col FROM t` per top-level column and store the bytes in the catalog."""
    con = con or catalog.connect()
    sid = con.execute("SELECT max(snapshot_id) FROM columns WHERE fqn = ?", [fqn]).fetchone()[0]
    if sid is None:
        raise SystemExit(f"{fqn} not in catalog; harvest its dataset first")
    cols = [r[0] for r in con.execute(
        "SELECT path FROM columns WHERE snapshot_id = ? AND fqn = ? AND path NOT LIKE '%.%'",
        [sid, fqn]).fetchall()]
    out = []
    for col in cols:
        b = dry_run(f"SELECT `{col}` FROM `{fqn}`")["bytes"]
        con.execute("UPDATE columns SET scan_bytes = ? WHERE snapshot_id = ? AND fqn = ? AND path = ?",
                    [b, sid, fqn, col])
        out.append((col, b))
    return out


def preview(fqn: str, n: int = 5) -> list[dict]:
    rows = client().list_rows(fqn, max_results=n)
    return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in rows]
