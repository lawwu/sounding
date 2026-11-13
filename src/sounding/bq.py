"""The only module that calls BigQuery.

Metadata, previews and user-SQL dry runs are free. The separate opt-in workload
scan executes fixed INFORMATION_SCHEMA SQL with a byte cap; never business SQL.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import logging
import json
import math
import hashlib
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

from google.api_core.exceptions import Forbidden, NotFound, GoogleAPIError, TooManyRequests
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery
from requests.exceptions import RequestException

from . import catalog, workload, recommendations
from .progress import Quiet
from .errors import SoundingError, integer, text, public_error

JOB_PROJECT = os.environ.get("SOUNDING_JOB_PROJECT")
GET_TABLE_WORKERS = 16  # concurrent tables.get / routines.get calls within one dataset
DATASET_WORKERS = 4     # datasets harvested at once; up to 4 x 16 requests in flight
DRY_RUN_WORKERS = 16    # concurrent column-cost dry runs, well under BigQuery's per-user API rate
MAX_SQL_LENGTH = 100_000
PRICE_BASIS = "US on-demand scan list price ($6.25/TiB); excludes free tier, discounts and other services."
PRICING_DOC = "https://cloud.google.com/bigquery/pricing#free"
COST_REPORT_MAX_HOURS = 180 * 24  # BigQuery keeps job history for 180 days
COST_REPORT_MAX_JOBS = 1000
PSEUDO_PARTITION_COLUMNS = {"_PARTITIONTIME", "_PARTITIONDATE"}  # ingestion-time partitioning
SAMPLE_ROWS = 10
SAMPLE_BYTES = 256 * 1024  # stored UTF-8 JSON per table, not a network transfer cap


def client() -> bigquery.Client:
    return bigquery.Client(project=JOB_PROJECT)


def project_datasets(project: str) -> dict:
    """Discover every visible, non-hidden dataset using the free paginated API."""
    workload.identifier(project, "project")
    c = client()
    datasets, unreachable = [], set()
    try:
        for page in c.list_datasets(project=project, page_size=1000, timeout=30).pages:
            unreachable.update((page._raw_page or {}).get("unreachable", []))
            datasets.extend(f"{d.project}.{d.dataset_id}" for d in page)
    finally:
        c.close()
    return {"datasets": sorted(set(datasets)), "unreachable": sorted(unreachable),
            "receipt": free_api_receipt("datasets.list")}


# Fields dropped before a REST resource is stored. View SQL is stored in its own
# column. Pass the original schema through to the catalog writer, which stores it
# once by content hash so newer field metadata survives without per-table copies.
# Dataset `access` lists the people and groups with access (emails),
# which a metadata catalog has no reason to keep.
TABLE_DROP = {"view", "etag", "selfLink", "id", "kind"}
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


OPTIONAL_ERRORS = (GoogleAPIError, GoogleAuthError, RequestException)


def metadata_failure(error):
    status = "denied" if isinstance(error, Forbidden) else "quota" if isinstance(error, TooManyRequests) else "unavailable"
    return status, {"denied": "Access was denied for this metadata.", "quota": "The metadata read quota was reached.",
                    "unavailable": "This metadata could not be read during refresh."}[status]


def model_record(r, dataset, status="harvested", message=None):
    ref = r["modelReference"]
    def columns(key):
        return [{"name": c.get("name"), "type": sql_type(c.get("type"))} for c in r.get(key, [])] if key in r else None
    return {"dataset": dataset, "model_id": ref["modelId"], "fqn": _ref(ref, "modelId"),
            "type": r.get("modelType"), "friendly_name": r.get("friendlyName"), "description": r.get("description"),
            "labels": r.get("labels"), "location": r.get("location"), "created": _ms(r.get("creationTime")),
            "modified": _ms(r.get("lastModifiedTime")), "expires": _ms(r.get("expirationTime")),
            "feature_columns": columns("featureColumns"), "label_columns": columns("labelColumns"),
            "remote_info": r.get("remoteModelInfo"), "metadata_status": status, "metadata_message": message}


def row_access_policies(c, table):
    """Paginated REST adapter: the Python BigQuery SDK has no list method yet."""
    if table.table_type not in ("TABLE", "SNAPSHOT"):
        return {"row_access_policies": None, "row_access_policies_status": "not_applicable", "row_access_policies_message": None}
    ref = table.reference
    path = "/projects/{}/datasets/{}/tables/{}/rowAccessPolicies".format(
        *(quote(v, safe="") for v in (ref.project, ref.dataset_id, ref.table_id)))
    policies, token, seen = [], None, set()
    try:
        while True:
            params = {"pageSize": 1000}
            if token:
                params["pageToken"] = token
            page = c._call_api(retry=None, method="GET", path=path, query_params=params, timeout=30)
            # Do not call getIamPolicy or store membership/grantee lists.
            policies.extend({k: p[k] for k in ("rowAccessPolicyReference", "filterPredicate", "creationTime", "lastModifiedTime") if k in p}
                            for p in page.get("rowAccessPolicies", []))
            token = page.get("nextPageToken")
            if not token:
                return {"row_access_policies": policies, "row_access_policies_status": "harvested", "row_access_policies_message": None}
            if token in seen:
                return {"row_access_policies": None, "row_access_policies_status": "unavailable",
                        "row_access_policies_message": "Row policy pagination did not complete."}
            seen.add(token)
    except OPTIONAL_ERRORS as error:
        status, message = metadata_failure(error)
        return {"row_access_policies": None, "row_access_policies_status": status, "row_access_policies_message": message}


def harvest(datasets: list[str], progress: Quiet | None = None, *, include_preview_rows: bool = False) -> dict:
    """Free metadata calls only, including models, row policies and cached recommendations.

    Datasets are crawled concurrently, each publishing its own snapshot, so a failure
    leaves only that dataset's previous snapshot current. Every dataset is attempted;
    if any failed, the first error is raised once the others finish. Returns a receipt
    counting the get calls made. None of them is a job.
    """
    progress = progress or Quiet()
    # Fail early on unsupported catalogs, but release the connection before network I/O.
    with catalog.connect():
        pass
    c = client()
    refs = []
    for raw_ds in datasets:  # validate every name before any network call
        text(raw_ds, "dataset")
        try:
            refs.append(bigquery.DatasetReference.from_string(raw_ds, default_project=c.project))
        except ValueError as error:
            raise SoundingError("Dataset must be dataset or project.dataset.") from error
    counts = {"datasets.get": 0, "tables.get": 0, "routines.get": 0, "models.get": 0}
    coverage = {}
    counts_lock = threading.Lock()
    progress.add(len(refs))  # one unit per dataset's list calls; tables and routines add theirs

    def crawl(ref) -> None:
        ds = str(ref)
        dataset_resource = c.get_dataset(ref).to_api_repr()
        table_refs = [i.reference for i in c.list_tables(ds)]
        progress.add(len(table_refs))
        with ThreadPoolExecutor(GET_TABLE_WORKERS) as pool:
            def get_table(r):
                try:
                    return c.get_table(r)
                except NotFound:  # deleted between tables.list and tables.get
                    progress.log(f"{ds}: {r.table_id} disappeared during the harvest; skipped")
                    return None
                finally:
                    progress.advance()
            fetched = [t for t in pool.map(get_table, table_refs) if t is not None]
            try:
                routine_refs = [i.reference for i in c.list_routines(ds)]
                progress.add(len(routine_refs))
                def get_routine(r):
                    try:
                        return c.get_routine(r)
                    except NotFound:
                        progress.log(f"{ds}: routine {r.routine_id} disappeared during the harvest; skipped")
                        return None
                    finally:
                        progress.advance()
                routines = [r for r in pool.map(get_routine, routine_refs) if r is not None]
                routines_status = "harvested"
            except Forbidden:
                # Routine access is a separate permission; tables are still worth keeping.
                routines, routines_status = [], "denied"
            model_rows, models_status, models_message = [], "harvested", None
            try:
                listed_models = list(c.list_models(ds, page_size=1000, timeout=30, retry=None))
                progress.add(len(listed_models))
                def get_model(model):
                    try:
                        return model_record(c.get_model(model.reference, timeout=30, retry=None).to_api_repr(), ds)
                    except OPTIONAL_ERRORS as error:
                        return model_record(model.to_api_repr(), ds, *metadata_failure(error))
                    finally:
                        progress.advance()
                model_rows = list(pool.map(get_model, listed_models))
                if any(m["metadata_status"] != "harvested" for m in model_rows):
                    models_status, models_message = "partial", "Model inventory was saved, but some model details were unavailable."
            except OPTIONAL_ERRORS as error:
                models_status, models_message = metadata_failure(error)
            progress.add(len(fetched))
            def get_policies(table):
                try:
                    return row_access_policies(c, table)
                finally:
                    progress.advance()
            policy_rows = list(pool.map(get_policies, fetched))
        table_rows, col_rows = [], []
        for t, policies in zip(fetched, policy_rows):
            table, columns = table_record(t.to_api_repr(), ds)
            table.update(policies)
            table["preview_status"] = ("pending" if include_preview_rows else "not_requested") if t.table_type in ("TABLE", "SNAPSHOT") else "not_applicable"
            table_rows.append(table)
            col_rows += columns
        routine_rows = [routine_record(r.to_api_repr(), ds) for r in routines]
        dataset_row = dataset_record(dataset_resource, ds, routines_status) | {"models_status": models_status, "models_message": models_message}
        sid = catalog.save_snapshot(ds, table_rows, col_rows, dataset_row, routine_rows, model_rows)
        progress.advance()  # Metadata is browseable before sample downloads finish.
        def get_sample(table):
            try:
                sample = table_sample(c, table)
                fqn = _ref(table.to_api_repr()["tableReference"])
                catalog.save_sample(fqn, sid, sample)
                return sample
            except Exception as error:
                logging.warning("Sample import failed (%s)", type(error).__name__)
                return {"preview_status": "unavailable"}
            finally:
                progress.advance()
        sample_rows = []
        if include_preview_rows:
            progress.add(len(fetched))
            with ThreadPoolExecutor(GET_TABLE_WORKERS) as pool:
                sample_rows = list(pool.map(get_sample, fetched))
        warnings = []
        if models_status != "harvested":
            warnings.append(models_message)
        if routines_status != "harvested":
            warnings.append("Routine metadata access was denied.")
        denied_policies = sum(p["row_access_policies_status"] not in ("harvested", "not_applicable") for p in policy_rows)
        if denied_policies:
            warnings.append(f"Row-access policy metadata was unavailable for {denied_policies} table(s).")
        denied_samples = sum(p["preview_status"] not in ("harvested", "not_applicable") for p in sample_rows)
        if denied_samples:
            warnings.append(f"Row samples were unavailable for {denied_samples} table(s).")
        progress.add()
        try:
            if dataset_resource.get("location"):
                scopes = recommendations.refresh(c, ref.project, dataset_resource["location"])
                warnings.extend(f"{s['kind'].title()}: {s['message']}" for s in scopes if s["status"] != "harvested")
            else:
                warnings.append("Recommendations were not collected because the dataset location is unknown.")
        except Exception as error:
            # The dataset has already been published; optional service/cache failures
            # must not misreport it as a failed snapshot or discard useful metadata.
            logging.warning("Recommendation refresh failed (%s)", type(error).__name__)
            warnings.append("Recommendation refresh did not complete; any previous cached results are retained.")
        finally:
            progress.advance()
        extra = f", {len(routine_rows)} routines" if routines_status == "harvested" else ", routines denied"
        progress.log(f"{ds}: {len(fetched)} tables, {len(col_rows)} column paths{extra}, {len(model_rows)} models (snapshot {sid})")
        with counts_lock:
            counts["datasets.get"] += 1
            counts["tables.get"] += len(fetched)
            counts["routines.get"] += len(routines)
            counts["models.get"] += len(model_rows)
            coverage[ds] = {"status": "partial" if warnings else "complete", "warnings": warnings}

    errors = []
    with ThreadPoolExecutor(min(DATASET_WORKERS, len(refs)) or 1) as pool:
        futures = {pool.submit(crawl, ref): str(ref) for ref in refs}
        for future, ds in futures.items():
            try:
                future.result()
            except Exception as error:
                errors.append(error)
                progress.log(f"{ds}: failed ({type(error).__name__}); its previous snapshot stays current")
    progress.close()
    c.close()
    if errors:
        raise errors[0]
    receipt = free_api_receipt("datasets.get", "tables.list", "tables.get", "routines.list", "routines.get",
                               "models.list", "models.get", "rowAccessPolicies.list", "recommendations.list", "insights.list",
                               *(["tabledata.list"] if include_preview_rows else []))
    receipt["source"] += " Recommender API reads are free (https://cloud.google.com/recommender/pricing); daily cached results may be reused."
    return receipt | {"get_calls": counts, "coverage": coverage}


def dry_run_receipt(job) -> dict:
    """Evidence that a dry run cost nothing, copied from BigQuery's own response.

    google-cloud-bigquery replaces a job's properties with the server's response, so
    these fields are what BigQuery returned, not what Sounding sent. A dry run comes
    back with configuration.dryRun true, statistics.query.totalBytesBilled "0" and no
    job ID, because no job is created. Anything else is reported, never smoothed over.
    """
    props = job._properties
    billed = props.get("statistics", {}).get("query", {}).get("totalBytesBilled")
    receipt = {"call": "jobs.insert (dry run)",
               "dry_run_confirmed": props.get("configuration", {}).get("dryRun") is True,
               "billed_bytes": None if billed is None else int(billed),
               "job_id": props.get("jobReference", {}).get("jobId"),
               "job_project": props.get("jobReference", {}).get("projectId"),
               "source": "BigQuery's response: configuration.dryRun, statistics.query.totalBytesBilled, jobReference.jobId"}
    receipt["free"] = receipt["dry_run_confirmed"] and receipt["billed_bytes"] == 0 and receipt["job_id"] is None
    return receipt


def combine_receipts(receipts: list[dict]) -> dict:
    """One receipt for several dry runs, such as a table's column costs."""
    billed = [r.get("billed_bytes") for r in receipts]
    return {"call": "jobs.insert (dry run)", "calls": len(receipts),
            "dry_run_confirmed": all(r.get("dry_run_confirmed") is True for r in receipts),
            "billed_bytes": None if None in billed else sum(billed),
            "jobs_created": sum(1 for r in receipts if r.get("job_id")),
            "free": all(r.get("free") is True for r in receipts),
            "source": "BigQuery's response to each dry run"}


def free_api_receipt(*calls: str) -> dict:
    """Metadata and tabledata.list calls are not jobs, and BigQuery returns no billing fields for them."""
    return {"call": ", ".join(calls), "jobs_created": 0, "free": True,
            "source": f"Not a job: BigQuery lists these calls as free ({PRICING_DOC}) and returns no "
                      "billing fields for them. `sounding cost-report` shows any job a project ran."}


def dry_run(sql: str) -> dict:
    text(sql, "sql", MAX_SQL_LENGTH)
    # Every statement, INFORMATION_SCHEMA included, is submitted only as a dry run, which
    # BigQuery never bills or executes; test_every_query_is_serialized_as_a_dry_run guards it.
    job = client().query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    receipt = dry_run_receipt(job)
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
    if not receipt["free"]:
        warnings.append("BigQuery's response did not confirm a free dry run (see receipt). Check `sounding cost-report`.")
    return {"bytes": reported if reliable else None, "reported_bytes": reported,
            "estimate_status": ("upper_bound" if accuracy == "UPPER_BOUND" else "estimated") if reliable else "unknown",
            "usd_on_demand": price["max"] if price and price["min"] == price["max"] else None,
            "usd_on_demand_range": price, "price_basis": PRICE_BASIS,
            "referenced_tables": refs, "referenced_tables_complete": complete_refs and reliable,
            "warnings": warnings, "source": "dry run (free; never executes SQL)", "receipt": receipt}


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


def column_costs(fqn: str, progress: Quiet | None = None) -> dict:
    """Compute outside the lock, then atomically cache only against the unchanged current snapshot.

    Returns {"columns": [(path, bytes)], "receipt": ...}, the receipt covering every dry run.
    """
    with catalog.connect() as con:
        table = catalog.describe(con, fqn)
    if table["type"] != "TABLE":
        raise SoundingError("Column costs require a native table.")
    cols = [c["path"] for c in table["columns"] if "." not in c["path"]]
    source = f"FROM {quote_identifier(fqn)} AS sounding_source"
    where, partition_col, warnings = "", None, []
    if table.get("require_partition_filter") and table.get("partitioning"):
        # BigQuery rejects queries on these tables without a partition filter. IS NOT NULL
        # satisfies it and prunes only the NULL partition. A real partition column then
        # adds its own bytes to every dry run, so they are measured once and subtracted.
        column = table["partitioning"].split(":", 1)[1]
        if column in PSEUDO_PARTITION_COLUMNS:
            where = f" WHERE {column} IS NOT NULL"
        else:
            partition_col = column
            where = f" WHERE sounding_source.{quote_identifier(column)} IS NOT NULL"
        warnings.append(f"This table requires a partition filter, so each dry run filters on {column} IS NOT "
                        "NULL; rows in the NULL partition, if any, are not counted.")
    progress = progress or Quiet()
    progress.add(len(cols) + (partition_col is not None and partition_col not in cols))

    def cost(col):
        result = dry_run(f"SELECT sounding_source.{quote_identifier(col)} {source}{where}")
        progress.advance()
        return col, result

    targets = cols + ([partition_col] if partition_col is not None and partition_col not in cols else [])
    with ThreadPoolExecutor(DRY_RUN_WORKERS) as pool:  # map keeps column order; the first failure is raised
        runs = list(pool.map(cost, targets))
    progress.close()
    measured = dict((col, r["bytes"]) for col, r in runs)
    base = measured.get(partition_col) if partition_col else 0
    out = [(col, measured[col] if col == partition_col or measured[col] is None or base is None
            else max(measured[col] - base, 0)) for col in cols]
    if partition_col and base is None:
        out = [(col, None if col != partition_col else b) for col, b in out]
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
    return {"columns": out, "warnings": warnings,
            "receipt": combine_receipts([r.get("receipt", {}) for _, r in runs])}


def _json_value(value):
    if hasattr(value, "items"):
        return {k: _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def table_sample(c, table):
    out = {"preview_rows": None, "preview_status": "not_applicable", "preview_message": None, "preview_sampled_at": None}
    if table.table_type not in ("TABLE", "SNAPSHOT"):
        return out
    try:
        rows, size = [], 2
        for row in c.list_rows(table, max_results=SAMPLE_ROWS, page_size=SAMPLE_ROWS, timeout=30, retry=None):
            value = _json_value(dict(row.items()))
            row_bytes = len(json.dumps(value).encode("utf-8")) + (2 if rows else 0)
            if size + row_bytes > SAMPLE_BYTES:
                out["preview_message"] = "Sample shortened to keep saved rows within 256 KB."
                break
            rows.append(value)
            size += row_bytes
        out.update(preview_rows=rows, preview_status="harvested", preview_sampled_at=dt.datetime.now(dt.UTC))
    except OPTIONAL_ERRORS as error:
        out["preview_status"], out["preview_message"] = metadata_failure(error)
    return out


def refresh_sample(fqn):
    """Replace a saved sample only if its table snapshot is still current."""
    with catalog.connect() as con:
        sid = catalog.describe(con, fqn)["snapshot_id"]
    c = client()
    try:
        sample = table_sample(c, c.get_table(fqn))
    finally:
        c.close()
    if not catalog.save_sample(fqn, sid, sample):
        raise SoundingError("The table was refreshed while loading its sample. Reload the table and try again.")
    with catalog.connect() as con:
        return catalog.describe(con, fqn) | {"receipt": free_api_receipt("tables.get", "tabledata.list")}


def preview(fqn: str, n: int = 5) -> dict:
    text(fqn, "table")
    integer(n, "n", 1, 50)
    c = client()
    table = c.get_table(fqn)
    if table.table_type not in ("TABLE", "SNAPSHOT"):
        raise SoundingError("Preview supports native tables and snapshots only, not views or external tables.")
    rows = [_json_value(dict(row.items())) for row in c.list_rows(table, max_results=n)]
    return {"rows": rows, "receipt": free_api_receipt("tables.get", "tabledata.list")}


def _iso(when: dt.datetime | None) -> str | None:
    return None if when is None else when.astimezone(dt.UTC).isoformat()


def cost_report(hours: int = 24) -> dict:
    """Every job your identity ran in the job project over the window, with BigQuery's billed bytes.

    jobs.list is free. Dry runs, metadata calls and tabledata.list never create jobs, so
    a window using only those free operations lists none; opt-in workload scans appear here
    with its own statistics.query.totalBytesBilled. Billing reports remain the final word.
    """
    integer(hours, "hours", 1, COST_REPORT_MAX_HOURS)
    c = client()
    now = dt.datetime.now(dt.UTC)
    since = now - dt.timedelta(hours=hours)
    try:
        jobs = list(c.list_jobs(min_creation_time=since, max_results=COST_REPORT_MAX_JOBS + 1))
    finally:
        c.close()
    truncated = len(jobs) > COST_REPORT_MAX_JOBS
    rows = []
    for job in jobs[:COST_REPORT_MAX_JOBS]:
        stats = job._properties.get("statistics", {})
        query = stats.get("query", {})
        billed = query.get("totalBytesBilled")
        config = job._properties.get("configuration", {}).get("query", {})
        sql = config.get("query")
        # Exact SQL + execution context, not a guessed normalized query family.
        group_key = hashlib.sha256(json.dumps([sql, config.get("defaultDataset"), config.get("useLegacySql"), job.location], sort_keys=True).encode()).hexdigest() if sql else None
        rows.append({"job_id": job.job_id, "project": job.project or c.project, "location": job.location,
                     "type": job.job_type, "state": job.state,
                     "query_key": group_key, "parent_job_id": stats.get("parentJobId"),
                     "created": _iso(job.created), "statement_type": query.get("statementType"),
                     "billed_bytes": None if billed is None else int(billed),
                     "cache_hit": query.get("cacheHit"), "slot_ms": _int(stats.get("totalSlotMs")),
                     "usd_on_demand": None if billed is None else catalog.usd(int(billed))})
    query_rows = [r for r in rows if r["type"] == "query" and not r["parent_job_id"]]
    groups = {}
    for row in query_rows:
        key = row["query_key"] or "job:" + row["job_id"]
        group = groups.setdefault(key, {"key": key, "executions": 0, "known_billed_bytes": 0, "known_slot_ms": 0,
            "unknown_billed": 0, "unknown_slots": 0, "cache_hits": 0, "representative": row, "grouped": row["query_key"] is not None})
        group["executions"] += 1
        group["known_billed_bytes"] += row["billed_bytes"] or 0
        group["known_slot_ms"] += row["slot_ms"] or 0
        group["unknown_billed"] += row["billed_bytes"] is None
        group["unknown_slots"] += row["slot_ms"] is None
        group["cache_hits"] += bool(row["cache_hit"])
        if (row["billed_bytes"] or 0) > (group["representative"]["billed_bytes"] or 0):
            group["representative"] = row
    ranked = sorted(groups.values(), key=lambda g: -g["known_billed_bytes"])
    for group in ranked:
        group["usd_on_demand"] = catalog.usd(group["known_billed_bytes"]) if not group["unknown_billed"] else None
    unknown = [r for r in query_rows if r["billed_bytes"] is None]
    billed_total = None if unknown else sum(r["billed_bytes"] for r in query_rows)
    warnings = []
    warnings.append("Query groups match exact SQL and execution context; different literals or formatting remain separate. Missing SQL stays ungrouped. Rankings cover only scanned jobs for this identity and job project; child jobs are excluded from totals to avoid double counting scripts.")
    if truncated:
        warnings.append(f"More than {COST_REPORT_MAX_JOBS} jobs; only the newest are listed. Use a shorter window.")
    if unknown:
        warnings.append("Some query jobs have no billed bytes yet (still running or failed early); the total is unknown.")
    if any(r["type"] != "query" for r in rows):
        warnings.append("Load, copy and extract jobs are listed but not priced here.")
    if any(r["slot_ms"] for r in query_rows):
        warnings.append("Under capacity (slot) pricing, cost follows slot usage, not billed bytes.")
    return {"project": c.project, "since": since.isoformat(), "until": now.isoformat(), "hours": hours,
            "job_count": len(rows), "query_jobs": len(query_rows), "billed_bytes": billed_total,
            "usd_on_demand": None if billed_total is None else catalog.usd(billed_total),
            "price_basis": PRICE_BASIS, "jobs": rows, "query_groups": ranked, "truncated": truncated, "warnings": warnings,
            "source": "BigQuery jobs.list (free) for jobs your identity ran in this project. Dry runs, "
                      "metadata calls and previews never create jobs, so they never appear. Jobs billed "
                      "to other projects are not shown; your billing report is the final word."}


def table_queries(table: str, days: int = 30, *, project: str | None = None,
                  scope: str = "user", rank_by: str = "billed_bytes", limit: int = 20) -> dict:
    """Rank individual completed query jobs that report a reference to this table.

    This is a bounded free jobs.list scan, not a query or a complete lineage log.
    No SQL text or identities are returned or saved; inspect_query_job can reveal
    SQL explicitly. Job totals are not attributed to the table's share of work.
    """
    text(table, "table")
    try:
        ref = bigquery.TableReference.from_string(table)
    except ValueError as error:
        raise SoundingError("Table must be project.dataset.table.") from error
    table = str(ref)
    integer(days, "days", 1, 180)
    integer(limit, "limit", 1, 100)
    if project is not None:
        workload.identifier(project, "project")
    if scope not in ("user", "project") or rank_by not in ("billed_bytes", "slot_ms"):
        raise SoundingError("Choose user or project scope, and billed_bytes or slot_ms ranking.")
    c = client()
    project = project or c.project
    now = dt.datetime.now(dt.UTC)
    since = now - dt.timedelta(days=days)
    jobs, unreachable = [], set()
    try:
        pages = c.list_jobs(project=project, all_users=scope == "project", state_filter="done",
                            min_creation_time=since, max_creation_time=now,
                            max_results=COST_REPORT_MAX_JOBS + 1, page_size=100, timeout=30).pages
        for page in pages:
            unreachable.update((page._raw_page or {}).get("unreachable", []))
            jobs.extend(page)
    finally:
        c.close()
    rows, missing_references, scripts = [], 0, 0
    for job in jobs[:COST_REPORT_MAX_JOBS]:
        props = job._properties
        if job.job_type != "query" or (props.get("status") or {}).get("errorResult"):
            continue
        stats = props.get("statistics") or {}
        query = stats.get("query") or {}
        if query.get("statementType") == "SCRIPT":
            scripts += 1
            continue
        refs = (query.get("referencedTables") or []) + (query.get("referencedLogicalViews") or [])
        if not refs:
            missing_references += 1
        if table not in {_ref(r) for r in refs}:
            continue
        rows.append({"job_id": job.job_id, "project": job.project, "location": job.location,
                     "created": _iso(job.created), "statement_type": query.get("statementType"),
                     "billed_bytes": _int(query.get("totalBytesBilled")),
                     "processed_bytes": _int(query.get("totalBytesProcessed")),
                     "slot_ms": _int(query.get("totalSlotMs", stats.get("totalSlotMs"))),
                     "cache_hit": query.get("cacheHit"), "referenced_tables": [_ref(r) for r in refs]})
    rows.sort(key=lambda r: (r[rank_by] is not None, r[rank_by] or 0, r["created"] or ""), reverse=True)
    truncated = len(jobs) > COST_REPORT_MAX_JOBS
    warnings = ["Ranks individual executions by whole-job totals, not the portion attributable to this table.",
                "Only top-level jobs with a reported table/view reference are matched. Script children and jobs in other projects are not searched; references can be incomplete."]
    if truncated:
        warnings.append(f"Only the newest {COST_REPORT_MAX_JOBS} completed jobs were scanned. Shorten the window; these are not necessarily the top jobs for the whole period.")
    if missing_references:
        warnings.append(f"{missing_references} query jobs had no visible table references and could not be matched (including possible cache hits or redacted metadata).")
    if unreachable:
        warnings.append("Google could not reach these locations: " + ", ".join(sorted(unreachable)))
    return {"table": table, "project": project, "scope": scope, "days": days,
            "since": since.isoformat(), "until": now.isoformat(), "rank_by": rank_by,
            "jobs_scanned": min(len(jobs), COST_REPORT_MAX_JOBS), "matching_jobs": len(rows),
            "jobs": rows[:limit], "truncated": truncated, "results_limited": len(rows) > limit,
            "missing_references": missing_references, "scripts_excluded": scripts,
            "warnings": warnings, "receipt": free_api_receipt("jobs.list")}


def query_opportunities(location: str, *, project: str | None = None, hours: int = 168,
                        scope: str = "user", rank_by: str = "billed_bytes", limit: int = 20,
                        max_bytes_billed: int = workload.DEFAULT_MAX_BYTES,
                        min_billed_bytes: int = workload.DEFAULT_MIN_BILLED_BYTES,
                        min_slot_ms: int = workload.DEFAULT_MIN_SLOT_MS,
                        allow_billed: bool = False) -> dict:
    """Preview by default; explicitly opt in to one capped metadata query and save it.

    Fixed SQL only. No job retries, business-query execution, or automatic widening
    of the time window, region, permissions or byte budget.
    """
    workload.validate(location, project, hours, scope, rank_by, limit, max_bytes_billed,
                      min_billed_bytes, min_slot_ms, allow_billed)
    # Check the catalog before spending, and release its lock before network I/O.
    if allow_billed:
        with catalog.connect():
            pass
    c = client()
    project = workload.identifier(project if project is not None else c.project, "project")
    until = dt.datetime.now(dt.UTC)
    since = until - dt.timedelta(hours=hours)
    sql = workload.query_sql(project, location, scope, rank_by)
    parameters = [bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in (
        ("since", "TIMESTAMP", since), ("until", "TIMESTAMP", until),
        ("min_billed_bytes", "INT64", min_billed_bytes), ("min_slot_ms", "INT64", min_slot_ms),
        ("limit", "INT64", limit))]
    config = bigquery.QueryJobConfig(
        dry_run=True, use_legacy_sql=False, use_query_cache=False,
        maximum_bytes_billed=max_bytes_billed, query_parameters=parameters,
        labels={"sounding_action": "query_opportunities"})
    preview = c.query(sql, job_config=config, location=location, job_retry=None)
    receipt = dry_run_receipt(preview)
    reported = preview.total_bytes_processed
    accuracy = _accuracy(preview._properties.get("statistics", {}).get("query", {}))
    estimated = reported if reported and reported > 0 and accuracy in ("PRECISE", "UPPER_BOUND") else None
    warnings = [
        "INFORMATION_SCHEMA scans are uncached and may be billed (10 MB minimum on demand).",
        "The byte cap limits on-demand scan charges; it is not a capacity/slot spending cap.",
        "Candidates are successful, non-cached query jobs created in this window and location; script parents and Sounding scans are excluded.",
        "Rankings use known totals, which can be incomplete. High usage and repeated queries are leads, not proof of waste or guaranteed savings.",
        "Normalized hashes group query templates, not identical results; they may change with underlying schemas or views.",
        "Dollar figures are US on-demand scan equivalents, not invoices; external services and model-training charges are not estimated.",
    ]
    if estimated is None:
        warnings.append("The dry-run estimate is unknown; zero or missing bytes do not mean execution is free. The execution byte cap still applies.")
    if not receipt["free"]:
        warnings.append("The preview response did not confirm a free dry run; execution is blocked.")
    over_budget = reported is not None and reported > max_bytes_billed
    if over_budget:
        warnings.append("The dry-run bytes exceed the cap. Shorten the window or explicitly increase the cap.")
    result = {"status": "preview", "project": project, "job_project": c.project,
              "location": location, "scope": scope, "since": since.isoformat(),
              "until": until.isoformat(), "hours": hours, "rank_by": rank_by, "limit": limit,
              "thresholds": {"min_billed_bytes": min_billed_bytes, "min_slot_ms": min_slot_ms},
              "max_bytes_billed": max_bytes_billed,
              "max_scan_usd_on_demand": catalog.usd(max_bytes_billed),
              "reported_bytes": reported, "estimated_bytes": estimated,
              "estimate_status": ("upper_bound" if accuracy == "UPPER_BOUND" else "estimated") if estimated else "unknown",
              "within_byte_cap": not over_budget if estimated is not None else None,
              "price_basis": PRICE_BASIS, "sql": sql, "warnings": warnings, "receipt": receipt}
    if not allow_billed:
        return result
    if not receipt["free"] or over_budget:
        result["status"] = "blocked"
        return result
    # Create a fresh config; do not mutate the one sent to the dry run.
    execution_config = bigquery.QueryJobConfig.from_api_repr(config.to_api_repr())
    execution_config.dry_run = False
    execution_config.job_timeout_ms = 60_000  # best effort, not a hard slot budget
    job = c.query(sql, job_config=execution_config, location=location, job_retry=None)
    execution_error = None
    try:
        rows = [_json_value(row) for row in job.result()]
    except Exception as error:
        # Keep the job identity and any available billing evidence after submission.
        # The operator can inspect it without paying for another scan.
        rows = []
        execution_error = public_error(error)[0]
    stats = job._properties.get("statistics", {}).get("query", {})
    billed = _int(stats.get("totalBytesBilled"))
    result.update(status="failed" if execution_error else "complete", preview_receipt=receipt,
                  opportunities=[workload.opportunity(row) for row in rows],
                  total_candidates=None if execution_error else (rows[0]["total_candidates"] if rows else 0),
                  truncated=bool(rows and rows[0]["total_candidates"] > len(rows)),
                  receipt={"call": "jobs.insert (INFORMATION_SCHEMA query)", "free": False,
                           "job_id": job.job_id, "job_project": job.project, "location": job.location,
                           "billed_bytes": billed, "processed_bytes": _int(stats.get("totalBytesProcessed")),
                           "slot_ms": _int(stats.get("totalSlotMs")),
                           "usd_on_demand_equivalent": catalog.usd(billed),
                           "maximum_bytes_billed": max_bytes_billed,
                           "state": job.state,
                           "source": "BigQuery job statistics; a metadata query was submitted."})
    if execution_error:
        result["error"] = execution_error
        warnings.append("The metadata query did not return a complete result. Inspect the receipt's job ID for status and billing; no replacement job was submitted.")
    elif not rows:
        warnings.append("No candidates in this scope. Check location, identity, window and thresholds; this does not establish zero project usage.")
    try:
        return catalog.save_workload_report(result)
    except Exception:
        # A local write failure must not discard a paid result and encourage another scan.
        logging.exception("Could not save completed workload report")
        result.update(report_id=None, saved_at=None)
        warnings.append("The scan completed but could not be saved locally. Keep this result; rerunning would execute another billed scan.")
        return result


def inspect_query_job(job_id: str, location: str, *, project: str | None = None,
                      include_sql: bool = False) -> dict:
    """Free jobs.get point lookup; report evidence without executing the query."""
    text(job_id, "job_id", 1024)
    workload.identifier(location, "location")
    if project is not None:
        workload.identifier(project, "project")
    if type(include_sql) is not bool:
        raise SoundingError("include_sql must be true or false.")
    c = client()
    job = c.get_job(job_id, project=project or c.project, location=location)
    if job.job_type != "query":
        raise SoundingError("The requested job is not a query job.")
    props = job._properties
    stats = props.get("statistics", {})
    query = stats.get("query", {})
    insights = query.get("performanceInsights") or {}
    findings = []
    for insight in insights.get("stagePerformanceStandaloneInsights", []):
        for api_key, signal in (("slotContention", "slot_contention"),
                                ("insufficientShuffleQuota", "insufficient_shuffle"),
                                ("highCardinalityJoins", "high_cardinality_join"),
                                ("partitionSkew", "partition_skew")):
            if insight.get(api_key):
                findings.append({"signal": signal, "stage_id": insight.get("stageId"),
                                 "evidence": insight[api_key], "next_step": workload.SIGNALS[signal]})
    stages = query.get("queryPlan", [])
    for stage in stages:
        spilled = _int(stage.get("shuffleOutputBytesSpilled"))
        read, written = _int(stage.get("recordsRead")), _int(stage.get("recordsWritten"))
        if spilled and spilled > 0:
            findings.append({"signal": "shuffle_spill", "stage_id": stage.get("id"),
                             "bytes": spilled, "next_step": workload.SIGNALS["insufficient_shuffle"]})
        if read and written is not None and written > 10 * read:
            findings.append({"signal": "row_expansion", "stage_id": stage.get("id"),
                             "ratio": written / read,
                             "next_step": "Check joins and UNNEST operations for unintended row multiplication; expansion can also be intentional."})
    sql = props.get("configuration", {}).get("query", {}).get("query")
    return {"project": job.project, "location": job.location, "job_id": job.job_id,
            "state": job.state, "statement_type": query.get("statementType"),
            "error_reason": (props.get("status", {}).get("errorResult") or {}).get("reason"),
            "created": _iso(job.created), "started": _iso(job.started), "ended": _iso(job.ended),
            "billed_bytes": _int(query.get("totalBytesBilled")),
            "processed_bytes": _int(query.get("totalBytesProcessed")),
            "slot_ms": _int(query.get("totalSlotMs")), "cache_hit": query.get("cacheHit"),
            "labels": props.get("configuration", {}).get("labels", {}),
            "referenced_tables": [_ref(r) for r in query.get("referencedTables", [])],
            "performance_insights": insights, "findings": findings,
            "stages": [{k: stage[k] for k in ("id", "name", "recordsRead", "recordsWritten", "slotMs",
                "shuffleOutputBytes", "shuffleOutputBytesSpilled", "waitRatioAvg", "computeRatioAvg") if k in stage}
                for stage in stages[:100]], "stages_truncated": len(stages) > 100,
            "sql": sql[:MAX_SQL_LENGTH] if include_sql and sql is not None else None,
            "sql_truncated": bool(include_sql and sql and len(sql) > MAX_SQL_LENGTH),
            "warnings": ["Missing stages or insights do not establish that a query is efficient; row-level security can hide execution statistics.",
                         "Inspect SQL and table metadata before applying a recommendation. Labels may identify a dbt model or owner."],
            "receipt": free_api_receipt("jobs.get")}
