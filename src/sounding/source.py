"""BigQuery reads for other Python programs: one table's metadata, and the query jobs of a time window.

Every call here is free (`tables.get`, `jobs.list`), returns plain records, and neither reads nor writes the
local catalog, so importing this module does not load DuckDB. The caller passes its own `bigquery.Client`
and closes it. This module is the public Python interface; see "Stability" in the README.
"""

from __future__ import annotations

import datetime as dt

from google.cloud import bigquery

from . import workload
from .errors import SoundingError, integer, text

PRICING_DOC = "https://cloud.google.com/bigquery/pricing#free"
JOB_HISTORY_DAYS = 180  # BigQuery keeps job history for 180 days

# View SQL is returned in its own field; the rest identify the REST response, not the table.
TABLE_DROP = {"view", "etag", "selfLink", "id", "kind"}
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


def _iso(when: dt.datetime | None) -> str | None:
    return None if when is None else when.astimezone(dt.UTC).isoformat()


def free_api_receipt(*calls: str) -> dict:
    """Metadata and tabledata.list calls are not jobs, and BigQuery returns no billing fields for them."""
    return {"call": ", ".join(calls), "jobs_created": 0, "free": True,
            "source": f"Not a job: BigQuery lists these calls as free ({PRICING_DOC}) and returns no "
                      "billing fields for them. `sounding cost-report` shows any job a project ran."}


def table(fqn: str, *, client: bigquery.Client) -> dict:
    """One table, view or snapshot as `tables.get` describes it now: {"table": row, "columns": [row, ...]}.

    Columns include RECORD parents and their children by dotted path. A snapshot or clone names its base in
    `base_table`, and `table["resource"]` keeps the rest of the REST response."""
    text(fqn, "table")
    try:
        ref = bigquery.TableReference.from_string(fqn)
    except ValueError as error:
        raise SoundingError("Table must be project.dataset.table.") from error
    row, columns = table_record(client.get_table(ref).to_api_repr(), f"{ref.project}.{ref.dataset_id}")
    return {"table": row, "columns": columns, "receipt": free_api_receipt("tables.get")}


def job_record(job, *, include_sql: bool = False) -> dict:
    """One query job from `jobs.list` as a flat record. Absent numbers stay None (unknown), never 0."""
    props = job._properties
    stats, status = props.get("statistics") or {}, props.get("status") or {}
    query = stats.get("query") or {}
    config = (props.get("configuration") or {}).get("query") or {}
    destination = config.get("destinationTable")
    record = {
        "project": job.project, "location": job.location, "job_id": job.job_id,
        "parent_job_id": stats.get("parentJobId"), "child_jobs": _int(stats.get("numChildJobs")) or 0,
        "created": _iso(_ms(stats.get("creationTime"))), "started": _iso(_ms(stats.get("startTime"))), "ended": _iso(_ms(stats.get("endTime"))),
        "state": status.get("state"), "statement_type": query.get("statementType"),
        "error_reason": (status.get("errorResult") or {}).get("reason"), "cache_hit": query.get("cacheHit"),
        "processed_bytes": _int(query.get("totalBytesProcessed")), "billed_bytes": _int(query.get("totalBytesBilled")),
        "slot_ms": _int(query.get("totalSlotMs", stats.get("totalSlotMs"))),
        "referenced_tables": sorted({_ref(r) for r in (query.get("referencedTables") or []) + (query.get("referencedLogicalViews") or [])}),
        "destination_table": _ref(destination),
        # query results land in a hidden, expiring dataset unless the job named a destination
        "destination_temporary": destination["datasetId"].startswith("_") if destination else None,
        "labels": (props.get("configuration") or {}).get("labels") or {},
    }
    return record | {"sql": config.get("query")} if include_sql else record


def job_pages(project: str, since: dt.datetime, until: dt.datetime, *, client: bigquery.Client, all_users: bool = True,
              include_sql: bool = False, page_size: int = 1000):
    """Yield every query job a project created in [since, until], a page at a time, newest first.

    Each page is {"jobs": [record, ...], "other_jobs": n, "unreachable": [location, ...], "receipt": ...}. A script
    is followed, in the same page, by its child jobs. Jobs of every state are reported: one still running will
    read differently later. This is `jobs.list`, which is free and has no query-pattern hash; `all_users` needs
    `bigquery.jobs.listAll`. SQL text is left out unless `include_sql`; error messages and user emails always
    are. A failure part-way raises, so a caller that records progress should do so only after the last page."""
    workload.identifier(project, "project")
    for name, when in (("since", since), ("until", until)):
        if not isinstance(when, dt.datetime) or when.tzinfo is None:
            raise SoundingError(f"{name} must be a datetime with a time zone.")
    if not since < until:
        raise SoundingError("since must be before until.")
    if until - since > dt.timedelta(days=JOB_HISTORY_DAYS):
        raise SoundingError(f"A window is at most {JOB_HISTORY_DAYS} days, the job history BigQuery keeps.")
    integer(page_size, "page_size", 1, 1000)
    listing = dict(project=project, page_size=page_size, timeout=30)
    for page in client.list_jobs(**listing, all_users=all_users, min_creation_time=since, max_creation_time=until).pages:
        jobs, other = [], 0
        for job in page:
            if job.job_type != "query":
                other += 1
                continue
            record = job_record(job, include_sql=include_sql)
            jobs.append(record)
            if record["child_jobs"]:
                # BigQuery rejects all_users together with a parent job; a script's children list without it
                for children in client.list_jobs(**listing, parent_job=job.job_id).pages:
                    jobs.extend(job_record(child, include_sql=include_sql) for child in children if child.job_type == "query")
        yield {"jobs": jobs, "other_jobs": other, "unreachable": sorted((page._raw_page or {}).get("unreachable", [])),
               "receipt": free_api_receipt("jobs.list")}
