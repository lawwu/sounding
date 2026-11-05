"""The only module that calls BigQuery: metadata, dry runs, and tabledata.list only.

No billed queries or Storage Read API calls are made, and the harvester never reads
INFORMATION_SCHEMA. User SQL, INFORMATION_SCHEMA included, is only ever dry-run.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery

from . import catalog
from .errors import SoundingError, integer, text

JOB_PROJECT = os.environ.get("SOUNDING_JOB_PROJECT")
GET_TABLE_WORKERS = 16
MAX_SQL_LENGTH = 100_000
PRICE_BASIS = "US on-demand scan list price ($6.25/TiB); excludes free tier, discounts and other services."


def client() -> bigquery.Client:
    return bigquery.Client(project=JOB_PROJECT)


def flatten(fields, prefix=""):
    """Yield every field, including RECORD parents and nested/repeated children."""
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


def harvest(datasets: list[str]) -> None:
    # Fail early on unsupported catalogs, but release the connection before network I/O.
    with catalog.connect():
        pass
    c = client()
    for raw_ds in datasets:
        text(raw_ds, "dataset")
        try:
            ds = str(bigquery.DatasetReference.from_string(raw_ds, default_project=c.project))
        except ValueError as error:
            raise SoundingError("Dataset must be dataset or project.dataset.") from error
        refs = [i.reference for i in c.list_tables(ds)]
        with ThreadPoolExecutor(GET_TABLE_WORKERS) as pool:
            fetched = list(pool.map(c.get_table, refs))
        table_rows, col_rows = [], []
        for t in fetched:
            fqn = f"{t.project}.{t.dataset_id}.{t.table_id}"
            table_rows.append({
                "dataset": ds, "table_id": t.table_id, "fqn": fqn,
                "type": t.table_type, "num_rows": t.num_rows, "num_bytes": t.num_bytes,
                "created": t.created, "modified": t.modified, "partitioning": _partitioning(t),
                "clustering": ",".join(t.clustering_fields) if t.clustering_fields else None,
                "description": t.description, "view_sql": t.view_query})
            col_rows += [{"fqn": fqn, "path": p, "type": f.field_type,
                          "mode": f.mode, "description": f.description} for p, f in flatten(t.schema)]
        sid = catalog.save_snapshot(ds, table_rows, col_rows)
        print(f"{ds}: {len(fetched)} tables, {len(col_rows)} column paths (snapshot {sid})",
              file=sys.stderr)


def dry_run(sql: str) -> dict:
    text(sql, "sql", MAX_SQL_LENGTH)
    # Every statement, INFORMATION_SCHEMA included, is submitted only as a dry run, which
    # BigQuery never bills or executes; test_every_query_is_serialized_as_a_dry_run guards it.
    job = client().query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    reported = job.total_bytes_processed
    stats = job._properties.get("statistics", {}).get("query", {})
    accuracy = _accuracy(stats)
    refs = [f"{r.project}.{r.dataset_id}.{r.table_id}" for r in job.referenced_tables]
    warnings = ["Dry-run estimates are not spending caps; data, pruning and runtime behavior can change costs."]
    reliable = job.statement_type == "SELECT" and reported is not None and reported > 0
    # Scripts, routines, external-service calls and incomplete byte estimates cannot be priced safely.
    reliable = reliable and accuracy in ("PRECISE", "UPPER_BOUND")
    reliable = reliable and not stats.get("referencedRoutines") and not stats.get("externalServiceCosts")
    if accuracy is None:
        warnings.append("BigQuery's response had no estimate-accuracy field, so the estimate is treated as unknown. "
                        "If every estimate says this, google-cloud-bigquery's response format has changed.")
    if not reliable and reported == 0 and refs and job.statement_type == "SELECT":
        warnings.append("Cost is unknown: BigQuery reported 0 bytes. The query may read no data, or it may read "
                        "external or row-level-secured tables whose cost a dry run cannot see.")
    elif not reliable:
        warnings.append("Cost is unknown: scripts, dynamic SQL, routines, external data and row-level security may be unestimated; zero reported bytes does not mean free execution.")
    complete_refs = job.statement_type == "SELECT" and len(refs) < 50
    price = catalog.billing_range(reported, len(refs)) if reliable and complete_refs else None
    if price and accuracy == "UPPER_BOUND":
        price["min"] = None
    if reliable and price is None:
        warnings.append("A scan-price range is unavailable because the referenced-table list may be incomplete.")
    if price and len(refs) > 1:
        warnings.append("Price is a range: the dry run does not report each table's share of billable bytes.")
    return {"bytes": reported if reliable else None, "reported_bytes": reported,
            "estimate_status": ("upper_bound" if accuracy == "UPPER_BOUND" else "estimated") if reliable else "unknown",
            "usd_on_demand": price["max"] if price and price["min"] == price["max"] else None,
            "usd_on_demand_range": price, "price_basis": PRICE_BASIS,
            "referenced_tables": refs, "referenced_tables_complete": complete_refs and reliable,
            "warnings": warnings, "source": "dry run (free; never executes SQL)"}


def _accuracy(stats: dict) -> str | None:
    """statistics.query.totalBytesProcessedAccuracy from the REST response.

    google-cloud-bigquery exposes no public property for it, so this reads the raw
    response. A missing field makes every estimate unknown (safe, but useless), so it
    is logged and reported rather than silently defaulted.
    """
    accuracy = stats.get("totalBytesProcessedAccuracy")
    if accuracy is None:
        logging.warning("dry run response has no totalBytesProcessedAccuracy; treating the estimate as unknown")
    return accuracy


def quote_identifier(name: str) -> str:
    """GoogleSQL quoted identifiers use backslash escapes, including for backticks."""
    text(name, "identifier")
    if any(ord(c) < 32 for c in name):
        raise SoundingError("Identifiers must not contain control characters.")
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def column_costs(fqn: str) -> list[tuple[str, int | None]]:
    """Compute outside the lock, then atomically cache only against the unchanged current snapshot."""
    with catalog.connect() as con:
        table = catalog.describe(con, fqn)
    if table["type"] != "TABLE":
        raise SoundingError("Column costs require a native table.")
    cols = [c["path"] for c in table["columns"] if "." not in c["path"]]
    out = [(col, dry_run(f"SELECT sounding_source.{quote_identifier(col)} FROM {quote_identifier(fqn)} AS sounding_source")["bytes"])
           for col in cols]
    with catalog.connect(write=True) as con:
        if catalog.describe(con, fqn)["snapshot_id"] != table["snapshot_id"]:
            raise SoundingError("The catalog changed while estimating columns. Retry against the new snapshot.")
        con.execute("BEGIN")
        try:
            for col, size in out:
                con.execute("UPDATE columns SET scan_bytes = ? WHERE snapshot_id = ? AND fqn = ? AND path = ?",
                            [size, table["snapshot_id"], fqn, col])
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return out


def _json_value(value):
    if isinstance(value, dict):
        return {k: _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    return value


def preview(fqn: str, n: int = 5) -> list[dict]:
    text(fqn, "table")
    integer(n, "n", 1, 50)
    c = client()
    table = c.get_table(fqn)
    if table.table_type not in ("TABLE", "SNAPSHOT"):
        raise SoundingError("Preview supports native tables and snapshots only, not views or external tables.")
    return [_json_value(dict(row.items())) for row in c.list_rows(table, max_results=n)]
