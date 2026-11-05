"""The only module that calls BigQuery: metadata (datasets, tables, routines), dry runs, and tabledata.list only.

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

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

from . import catalog
from .errors import SoundingError, integer, text

JOB_PROJECT = os.environ.get("SOUNDING_JOB_PROJECT")
GET_TABLE_WORKERS = 16
MAX_SQL_LENGTH = 100_000
PRICE_BASIS = "US on-demand scan list price ($6.25/TiB); excludes free tier, discounts and other services."


def client() -> bigquery.Client:
    return bigquery.Client(project=JOB_PROJECT)


# Fields dropped before a REST resource is stored. The schema and view SQL are stored in
# their own columns. Dataset `access` lists the people and groups with access (emails),
# which a metadata catalog has no reason to keep.
TABLE_DROP = {"schema", "view", "etag", "selfLink", "id", "kind"}
DATASET_DROP = {"access", "etag", "selfLink", "id", "kind"}
ROUTINE_DROP = {"etag"}
STORAGE_FIELDS = {  # tables.get returns these by default; any may be absent (unknown).
    "numActiveLogicalBytes": "active_logical", "numLongTermLogicalBytes": "long_term_logical",
    "numTotalLogicalBytes": "total_logical", "numActivePhysicalBytes": "active_physical",
    "numLongTermPhysicalBytes": "long_term_physical", "numTotalPhysicalBytes": "total_physical",
    "numTimeTravelPhysicalBytes": "time_travel_physical", "numCurrentPhysicalBytes": "current_physical",
}


def flatten(fields: list[dict], prefix=""):
    """Yield (path, field) for every REST schema field, including RECORD parents and children."""
    for f in fields:
        path = f"{prefix}{f['name']}"
        yield path, f
        if f.get("type") in ("RECORD", "STRUCT"):
            yield from flatten(f.get("fields", []), path + ".")


def _int(value) -> int | None:
    """REST int64 fields arrive as strings; absent stays None (unknown), never 0."""
    return None if value is None else int(value)


def _ms(value) -> dt.datetime | None:
    return None if value is None else dt.datetime.fromtimestamp(int(value) / 1000, dt.UTC)


def _ref(r: dict | None, key="tableId") -> str | None:
    return f"{r['projectId']}.{r['datasetId']}.{r[key]}" if r else None


def _partitioning(r: dict) -> str | None:
    if tp := r.get("timePartitioning"):
        return f"{tp['type']}:{tp.get('field') or '_PARTITIONTIME'}"
    if rp := r.get("rangePartitioning"):
        return f"RANGE:{rp['field']}"
    return None


def sql_type(t: dict | None) -> str | None:
    """A GoogleSQL type name from a REST StandardSqlDataType."""
    if not t:
        return None
    kind = t.get("typeKind")
    if kind == "ARRAY":
        return f"ARRAY<{sql_type(t.get('arrayElementType'))}>"
    if kind == "STRUCT":
        fields = t.get("structType", {}).get("fields", [])
        return "STRUCT<" + ", ".join(f"{f.get('name', '')} {sql_type(f.get('type'))}".strip() for f in fields) + ">"
    if kind == "RANGE":
        return f"RANGE<{sql_type(t.get('rangeElementType'))}>"
    return kind


def table_record(r: dict, dataset: str) -> tuple[dict, list[dict]]:
    """One tables.get response as a catalog row and its column rows."""
    fqn = _ref(r["tableReference"])
    partitioning = _partitioning(r)
    constraints = r.get("tableConstraints") or {}
    copy = r.get("snapshotDefinition") or r.get("cloneDefinition")
    mv = r.get("materializedView")
    storage = {name: int(r[key]) for key, name in STORAGE_FIELDS.items() if r.get(key) is not None}
    # requirePartitionFilter moved from timePartitioning to the table; BigQuery omits false.
    require = r.get("requirePartitionFilter", (r.get("timePartitioning") or {}).get("requirePartitionFilter"))
    table = {
        "dataset": dataset, "table_id": r["tableReference"]["tableId"], "fqn": fqn, "type": r.get("type"),
        "num_rows": _int(r.get("numRows")), "num_bytes": _int(r.get("numBytes")),
        "created": _ms(r.get("creationTime")), "modified": _ms(r.get("lastModifiedTime")),
        "partitioning": partitioning,
        "clustering": ",".join(r["clustering"]["fields"]) if r.get("clustering") else None,
        "description": r.get("description"),
        "view_sql": (r.get("view") or {}).get("query") or (mv or {}).get("query"),
        "location": r.get("location"), "expires": _ms(r.get("expirationTime")),
        "labels": r.get("labels") or {},
        "require_partition_filter": bool(require) if partitioning else None,
        "partition_expiration_ms": _int((r.get("timePartitioning") or {}).get("expirationMs")),
        "num_partitions": _int(r.get("numPartitions")),
        "primary_key": (constraints.get("primaryKey") or {}).get("columns"),
        "foreign_keys": [{"name": fk.get("name"), "references": _ref(fk.get("referencedTable")),
                          "columns": [[c["referencingColumn"], c["referencedColumn"]] for c in fk.get("columnReferences", [])]}
                         for fk in constraints.get("foreignKeys", [])] or None,
        "storage": storage or None,
        "kms_key": (r.get("encryptionConfiguration") or {}).get("kmsKeyName"),
        "base_table": _ref((copy or {}).get("baseTableReference")),
        "base_kind": ("SNAPSHOT" if r.get("snapshotDefinition") else "CLONE") if copy else None,
        "external": r.get("externalDataConfiguration"),
        "materialized_view": ({k: v for k, v in mv.items() if k != "query"} | {"status": r.get("materializedViewStatus")}
                              if mv else None),
        "resource": {k: v for k, v in r.items() if k not in TABLE_DROP},
    }
    columns = [{"fqn": fqn, "path": path, "type": f.get("type"), "mode": f.get("mode", "NULLABLE"),
                "description": f.get("description"), "policy_tags": (f.get("policyTags") or {}).get("names")}
               for path, f in flatten((r.get("schema") or {}).get("fields", []))]
    return table, columns


def dataset_record(r: dict, dataset: str, routines_status: str) -> dict:
    return {"dataset": dataset, "location": r.get("location"), "friendly_name": r.get("friendlyName"),
            "description": r.get("description"), "labels": r.get("labels") or {},
            "created": _ms(r.get("creationTime")), "modified": _ms(r.get("lastModifiedTime")),
            "default_table_expiration_ms": _int(r.get("defaultTableExpirationMs")),
            "default_partition_expiration_ms": _int(r.get("defaultPartitionExpirationMs")),
            "storage_billing_model": r.get("storageBillingModel"),
            "max_time_travel_hours": _int(r.get("maxTimeTravelHours")), "routines_status": routines_status,
            "resource": {k: v for k, v in r.items() if k not in DATASET_DROP}}


def routine_record(r: dict, dataset: str) -> dict:
    ref = r["routineReference"]
    return {"dataset": dataset, "routine_id": ref["routineId"], "fqn": _ref(ref, "routineId"),
            "type": r.get("routineType"), "language": r.get("language"),
            "arguments": [{"name": a.get("name"), "type": sql_type(a.get("dataType")),
                           "mode": a.get("mode"), "kind": a.get("argumentKind")} for a in r.get("arguments", [])],
            "return_type": sql_type(r.get("returnType")) or (
                "TABLE<" + ", ".join(f"{c['name']} {sql_type(c.get('type'))}" for c in r["returnTableType"]["columns"]) + ">"
                if r.get("returnTableType") else None),
            "description": r.get("description"), "definition": r.get("definitionBody"),
            "created": _ms(r.get("creationTime")), "modified": _ms(r.get("lastModifiedTime")),
            "resource": {k: v for k, v in r.items() if k not in ROUTINE_DROP}}


def harvest(datasets: list[str]) -> None:
    """Free metadata calls only: datasets.get, tables.list/get and routines.list/get."""
    # Fail early on unsupported catalogs, but release the connection before network I/O.
    with catalog.connect():
        pass
    c = client()
    for raw_ds in datasets:
        text(raw_ds, "dataset")
        try:
            ref = bigquery.DatasetReference.from_string(raw_ds, default_project=c.project)
        except ValueError as error:
            raise SoundingError("Dataset must be dataset or project.dataset.") from error
        ds = str(ref)
        dataset_resource = c.get_dataset(ref).to_api_repr()
        refs = [i.reference for i in c.list_tables(ds)]
        with ThreadPoolExecutor(GET_TABLE_WORKERS) as pool:
            fetched = list(pool.map(c.get_table, refs))
            try:
                routine_refs = [i.reference for i in c.list_routines(ds)]
                routines = list(pool.map(c.get_routine, routine_refs))
                routines_status = "harvested"
            except Forbidden:
                # Routine access is a separate permission; tables are still worth keeping.
                routines, routines_status = [], "denied"
        table_rows, col_rows = [], []
        for t in fetched:
            table, columns = table_record(t.to_api_repr(), ds)
            table_rows.append(table)
            col_rows += columns
        routine_rows = [routine_record(r.to_api_repr(), ds) for r in routines]
        sid = catalog.save_snapshot(ds, table_rows, col_rows,
                                    dataset_record(dataset_resource, ds, routines_status), routine_rows)
        extra = f", {len(routine_rows)} routines" if routines_status == "harvested" else ", routines denied"
        print(f"{ds}: {len(fetched)} tables, {len(col_rows)} column paths{extra} (snapshot {sid})",
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
