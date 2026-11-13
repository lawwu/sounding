"""Local metadata snapshots; no BigQuery calls.

An advisory sidecar lock coordinates all Sounding processes. Connections are short
and never held across network calls. Readers open DuckDB read-only after initialization.
"""

from __future__ import annotations

import datetime as dt
import json
import hashlib
import os
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa
from filelock import FileLock, Timeout

from .errors import CatalogBusy, CatalogVersionError, MissingTable, integer, text

CATALOG = Path(os.environ.get("SOUNDING_CATALOG", Path.home() / ".sounding" / "catalog.duckdb"))
LOCK_TIMEOUT = 10
SCHEMA_VERSION = 6
USD_PER_TIB = 6.25  # US on-demand list price; excludes discounts and the monthly free tier.
TIB = 1024**4
MIB = 1024**2
SEARCH_LIMIT = 100
MATCH_LIMIT = 20

# Schema 2 as first released. Later versions are applied on top by MIGRATIONS, so a new
# catalog and an upgraded one end up identical.
SCHEMA_V2 = """
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

# Additive only: snapshots harvested before an upgrade keep NULL (unknown) in new fields.
MIGRATIONS = {
    2: """
ALTER TABLE tables ADD COLUMN location VARCHAR;
ALTER TABLE tables ADD COLUMN expires TIMESTAMPTZ;
ALTER TABLE tables ADD COLUMN labels JSON;
ALTER TABLE tables ADD COLUMN require_partition_filter BOOLEAN;
ALTER TABLE tables ADD COLUMN partition_expiration_ms BIGINT;
ALTER TABLE tables ADD COLUMN num_partitions BIGINT;
ALTER TABLE tables ADD COLUMN primary_key VARCHAR[];
ALTER TABLE tables ADD COLUMN foreign_keys JSON;
ALTER TABLE tables ADD COLUMN storage JSON;
ALTER TABLE tables ADD COLUMN kms_key VARCHAR;
ALTER TABLE tables ADD COLUMN base_table VARCHAR;
ALTER TABLE tables ADD COLUMN base_kind VARCHAR;
ALTER TABLE tables ADD COLUMN external JSON;
ALTER TABLE tables ADD COLUMN materialized_view JSON;
ALTER TABLE tables ADD COLUMN resource JSON;
ALTER TABLE columns ADD COLUMN policy_tags VARCHAR[];
CREATE TABLE datasets (
  snapshot_id BIGINT PRIMARY KEY, dataset VARCHAR, location VARCHAR, friendly_name VARCHAR,
  description VARCHAR, labels JSON, created TIMESTAMPTZ, modified TIMESTAMPTZ,
  default_table_expiration_ms BIGINT, default_partition_expiration_ms BIGINT,
  storage_billing_model VARCHAR, max_time_travel_hours BIGINT, routines_status VARCHAR,
  resource JSON);
CREATE TABLE routines (
  snapshot_id BIGINT, dataset VARCHAR, routine_id VARCHAR, fqn VARCHAR, type VARCHAR,
  language VARCHAR, arguments JSON, return_type VARCHAR, description VARCHAR,
  definition VARCHAR, created TIMESTAMPTZ, modified TIMESTAMPTZ, resource JSON,
  PRIMARY KEY (snapshot_id, fqn));
UPDATE catalog_meta SET version = 3;
""",
    3: """
CREATE TABLE workload_reports (
  report_id VARCHAR PRIMARY KEY, saved_at TIMESTAMPTZ NOT NULL, report JSON NOT NULL);
UPDATE catalog_meta SET version = 4;
""",
    4: """
ALTER TABLE datasets ADD COLUMN models_status VARCHAR;
ALTER TABLE datasets ADD COLUMN models_message VARCHAR;
ALTER TABLE tables ADD COLUMN row_access_policies JSON;
ALTER TABLE tables ADD COLUMN row_access_policies_status VARCHAR;
ALTER TABLE tables ADD COLUMN row_access_policies_message VARCHAR;
ALTER TABLE tables ADD COLUMN preview_rows JSON;
ALTER TABLE tables ADD COLUMN preview_status VARCHAR;
ALTER TABLE tables ADD COLUMN preview_message VARCHAR;
ALTER TABLE tables ADD COLUMN preview_sampled_at TIMESTAMPTZ;
CREATE TABLE models (
  snapshot_id BIGINT, dataset VARCHAR, model_id VARCHAR, fqn VARCHAR, type VARCHAR,
  friendly_name VARCHAR, description VARCHAR, labels JSON, location VARCHAR,
  created TIMESTAMPTZ, modified TIMESTAMPTZ, expires TIMESTAMPTZ,
  feature_columns JSON, label_columns JSON, remote_info JSON, metadata_status VARCHAR,
  metadata_message VARCHAR, PRIMARY KEY (snapshot_id, fqn));
CREATE TABLE recommendation_cache (
  project VARCHAR, location VARCHAR, kind VARCHAR, checked_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ, status VARCHAR, message VARCHAR, items JSON,
  PRIMARY KEY (project, location, kind));
CREATE TABLE projects (project VARCHAR PRIMARY KEY, imported_at TIMESTAMPTZ);
UPDATE catalog_meta SET version = 5;
""",
    5: """
CREATE TABLE schemas (schema_hash VARCHAR PRIMARY KEY, schema JSON NOT NULL);
ALTER TABLE tables ADD COLUMN schema_hash VARCHAR;
UPDATE catalog_meta SET version = 6;
""",
}
JSON_FIELDS = {"labels", "foreign_keys", "storage", "external", "materialized_view", "resource", "arguments",
               "row_access_policies", "feature_columns", "label_columns", "remote_info", "items", "preview_rows"}

LATEST = """WITH latest AS (
    SELECT dataset, max(snapshot_id) AS snapshot_id FROM snapshots GROUP BY dataset
), current_tables AS (
    SELECT t.* FROM tables t JOIN latest l USING (dataset, snapshot_id)
)"""

_TS = pa.timestamp("us", tz="UTC")
_LIST = pa.list_(pa.string())
# JSON columns travel as strings; DuckDB casts them on insert.
TABLE_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("dataset", pa.string()), ("table_id", pa.string()),
    ("fqn", pa.string()), ("type", pa.string()), ("num_rows", pa.int64()),
    ("num_bytes", pa.int64()), ("created", _TS), ("modified", _TS), ("partitioning", pa.string()),
    ("clustering", pa.string()), ("description", pa.string()), ("view_sql", pa.string()),
    ("location", pa.string()), ("expires", _TS), ("labels", pa.string()),
    ("require_partition_filter", pa.bool_()), ("partition_expiration_ms", pa.int64()),
    ("num_partitions", pa.int64()), ("primary_key", _LIST), ("foreign_keys", pa.string()),
    ("storage", pa.string()), ("kms_key", pa.string()), ("base_table", pa.string()),
    ("base_kind", pa.string()), ("external", pa.string()), ("materialized_view", pa.string()),
    ("resource", pa.string()),
    ("row_access_policies", pa.string()), ("row_access_policies_status", pa.string()),
    ("row_access_policies_message", pa.string()),
    ("preview_rows", pa.string()), ("preview_status", pa.string()), ("preview_message", pa.string()),
    ("preview_sampled_at", _TS),
    ("schema_hash", pa.string()),
])
COLUMN_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("fqn", pa.string()), ("path", pa.string()),
    ("type", pa.string()), ("mode", pa.string()), ("description", pa.string()),
    ("scan_bytes", pa.int64()), ("policy_tags", _LIST),
])
DATASET_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("dataset", pa.string()), ("location", pa.string()),
    ("friendly_name", pa.string()), ("description", pa.string()), ("labels", pa.string()),
    ("created", _TS), ("modified", _TS), ("default_table_expiration_ms", pa.int64()),
    ("default_partition_expiration_ms", pa.int64()), ("storage_billing_model", pa.string()),
    ("max_time_travel_hours", pa.int64()), ("routines_status", pa.string()), ("resource", pa.string()),
    ("models_status", pa.string()), ("models_message", pa.string()),
])
ROUTINE_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("dataset", pa.string()), ("routine_id", pa.string()),
    ("fqn", pa.string()), ("type", pa.string()), ("language", pa.string()),
    ("arguments", pa.string()), ("return_type", pa.string()), ("description", pa.string()),
    ("definition", pa.string()), ("created", _TS), ("modified", _TS), ("resource", pa.string()),
])
MODEL_SCHEMA = pa.schema([
    ("snapshot_id", pa.int64()), ("dataset", pa.string()), ("model_id", pa.string()),
    ("fqn", pa.string()), ("type", pa.string()), ("friendly_name", pa.string()),
    ("description", pa.string()), ("labels", pa.string()), ("location", pa.string()),
    ("created", _TS), ("modified", _TS), ("expires", _TS),
    ("feature_columns", pa.string()), ("label_columns", pa.string()), ("remote_info", pa.string()),
    ("metadata_status", pa.string()), ("metadata_message", pa.string()),
])


# Paths already checked (or created) at SCHEMA_VERSION in this process, so each request
# doesn't pay for a second DuckDB open just to re-read the version.
_VERIFIED: set[Path] = set()


def _version(con) -> int | None:
    """The catalog's schema version; None for a pre-versioning (schema 1) catalog."""
    exists = con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'catalog_meta'").fetchone()[0]
    rows = con.execute("SELECT version FROM catalog_meta").fetchall() if exists else []
    return rows[0][0] if len(rows) == 1 else None


def _set_aside(path: Path, version: int | None) -> Path:
    """Rename an older catalog (and its WAL) next to itself. Nothing is deleted.

    The catalog is a cache that a harvest can rebuild, and older schemas can't be
    converted reliably (timezone-less timestamps, alias-keyed snapshots), so the
    default path is reused for a fresh catalog rather than asking every CLI, UI and
    MCP process to point somewhere new.
    """
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")
    target = path.with_name(f"{path.stem}.v{version or 1}-{stamp}{path.suffix}")
    os.replace(path, target)
    wal = Path(str(path) + ".wal")
    if wal.exists():
        os.replace(wal, Path(str(target) + ".wal"))
    print(f"sounding: catalog schema {version or 1} is older than {SCHEMA_VERSION}; moved it to {target} "
          "and started a new catalog. Re-run `sounding harvest` for your datasets.", file=sys.stderr)
    return target


def _migrate(path: Path, version: int) -> None:
    """Apply additive migrations in place, in one transaction per step."""
    original_version = version
    with _open(path, write=True) as con:
        while version < SCHEMA_VERSION:
            con.execute("BEGIN")
            try:
                con.execute(MIGRATIONS[version])
                if version == 5:
                    _deduplicate_schemas(con)
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
            version = _version(con)
    message = "Schema storage is deduplicated; existing data and snapshots are unchanged."
    if original_version < 5:
        message += " Fields introduced after your original version remain unknown until harvested."
    print(f"sounding: upgraded the catalog to schema {SCHEMA_VERSION}. {message}", file=sys.stderr)


def _extract_schema(row):
    """Return a new row and canonical schema; preserve array order and every field."""
    row = dict(row)
    resource = row.get("resource")
    if isinstance(resource, str):
        resource = json.loads(resource)
    schema = resource.get("schema") if resource is not None else None
    if schema is None:
        row["schema_hash"] = None
        return row, None
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    row["schema_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    row["resource"] = {k: v for k, v in resource.items() if k != "schema"}
    return row, canonical


def _store_schemas(con, schemas):
    if not schemas:
        return
    batch = pa.table({"schema_hash": list(schemas), "schema": list(schemas.values())})
    con.register("schema_batch", batch)
    try:
        con.execute('INSERT INTO schemas SELECT schema_hash, schema::JSON FROM schema_batch ON CONFLICT DO NOTHING')
    finally:
        con.unregister("schema_batch")


def _deduplicate_schemas(con, batch_size=256):
    """Bounded batches within the caller's migration transaction; safe to rerun."""
    last = -1
    while True:
        rows = con.execute("""SELECT rowid, resource FROM tables
            WHERE rowid > ? AND schema_hash IS NULL AND resource IS NOT NULL
            ORDER BY rowid LIMIT ?""", [last, batch_size]).fetchall()
        if not rows:
            break
        last = rows[-1][0]
        schemas, updates = {}, []
        for rowid, resource in rows:
            row, schema = _extract_schema({"resource": resource})
            if schema is not None:
                schemas[row["schema_hash"]] = schema
                updates.append({"rid": rowid, "schema_hash": row["schema_hash"], "resource": json.dumps(row["resource"], sort_keys=True)})
        _store_schemas(con, schemas)
        if updates:
            con.register("schema_updates", pa.Table.from_pylist(updates))
            try:
                con.execute("""UPDATE tables SET schema_hash=u.schema_hash, resource=u.resource::JSON
                    FROM schema_updates u WHERE tables.rowid=u.rid""")
            finally:
                con.unregister("schema_updates")


def _open(path: Path, write: bool):
    try:
        return duckdb.connect(str(path), read_only=not write)
    except duckdb.IOException as error:
        raise CatalogBusy("Catalog is in use by another program. Close it and retry.") from error


@contextmanager
def connect(path: Path | str | None = None, *, write: bool = False):
    """Acquire the process/thread lock before opening DuckDB; always close before unlocking.

    A schema-2 catalog is upgraded in place (see MIGRATIONS). A schema-1 catalog can't be
    converted, so it is moved aside (see _set_aside) and replaced by an empty one. A
    newer-schema catalog is left untouched and refused.
    """
    path = Path(path or CATALOG).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(path) + ".lock", timeout=LOCK_TIMEOUT):
            if path.exists() and path not in _VERIFIED:
                with _open(path, write=False) as probe:
                    version = _version(probe)
                if version is not None and version > SCHEMA_VERSION:
                    raise CatalogVersionError(
                        f"This catalog uses schema {version}, newer than this Sounding supports "
                        f"({SCHEMA_VERSION}). Upgrade Sounding, or set SOUNDING_CATALOG to another file. "
                        "No data was changed.")
                if version in MIGRATIONS:
                    _migrate(path, version)
                elif version != SCHEMA_VERSION:
                    _set_aside(path, version)
            if not path.exists():
                with duckdb.connect(str(path)) as initial:
                    initial.execute(SCHEMA_V2)
                    for step in range(2, SCHEMA_VERSION):
                        initial.execute(MIGRATIONS[step])
            _VERIFIED.add(path)
            con = _open(path, write)
            try:
                con.execute("SET TimeZone = 'UTC'")
                yield con
            finally:
                con.close()
    except Timeout as error:
        raise CatalogBusy("Catalog is busy. Retry after the current local operation finishes.") from error


def save_snapshot(dataset: str, table_rows: list[dict], col_rows: list[dict],
                  dataset_row: dict | None = None, routine_rows: list[dict] = (),
                  model_rows: list[dict] = ()) -> int:
    """Publish one complete dataset atomically. Failed inserts leave the previous snapshot current."""
    batches = [("tables", table_rows, TABLE_SCHEMA), ("columns", col_rows, COLUMN_SCHEMA),
               ("datasets", [dataset_row] if dataset_row else [], DATASET_SCHEMA),
               ("routines", list(routine_rows), ROUTINE_SCHEMA),
               ("models", list(model_rows), MODEL_SCHEMA)]
    with connect(write=True) as con:
        con.execute("BEGIN")
        try:
            sid = con.execute("SELECT coalesce(max(snapshot_id), 0) + 1 FROM snapshots").fetchone()[0]
            con.execute("INSERT INTO snapshots VALUES (?, ?, ?)", [sid, dt.datetime.now(dt.UTC), dataset])
            for name, rows, schema in batches:
                if rows:
                    if name == "tables":
                        prepared, schemas = [], {}
                        for row in rows:
                            row, canonical = _extract_schema(row)
                            prepared.append(row)
                            if canonical is not None:
                                schemas[row["schema_hash"]] = canonical
                        _store_schemas(con, schemas)
                        rows = prepared
                    batch = pa.Table.from_pylist([_encode(dict(row, snapshot_id=sid)) for row in rows], schema=schema)
                    con.register("batch", batch)
                    # Explicit Arrow fields and BY NAME avoid depending on dict/DDL column ordering.
                    con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM batch")
                    con.unregister("batch")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return sid


def _encode(row: dict) -> dict:
    return {k: json.dumps(v, sort_keys=True) if k in JSON_FIELDS and v is not None else v
            for k, v in row.items()}


def save_sample(fqn, snapshot_id, sample):
    """Attach data to its metadata snapshot; a slower import cannot alter a newer one."""
    with connect(write=True) as con:
        row = con.execute(LATEST + " SELECT snapshot_id FROM current_tables WHERE fqn=?", [fqn]).fetchone()
        if not row or row[0] != snapshot_id:
            return False
        con.execute("UPDATE tables SET preview_rows=?, preview_status=?, preview_message=?, preview_sampled_at=? WHERE snapshot_id=? AND fqn=?",
                    [json.dumps(sample["preview_rows"]) if sample["preview_rows"] is not None else None,
                     sample["preview_status"], sample["preview_message"], sample["preview_sampled_at"], snapshot_id, fqn])
        return True


def save_workload_report(report: dict) -> dict:
    """Store a completed bounded scan, including its billing receipt, in one insert."""
    saved = report | {"report_id": uuid.uuid4().hex, "saved_at": dt.datetime.now(dt.UTC).isoformat()}
    with connect(write=True) as con:
        con.execute("INSERT INTO workload_reports VALUES (?, ?, ?)",
                    [saved["report_id"], saved["saved_at"], json.dumps(saved)])
    return saved


def workload_report(con, report_id: str | None = None) -> dict:
    """An exact report or the latest across all scopes; no BigQuery call or refresh."""
    if report_id is not None:
        text(report_id, "report_id", 128)
        row = con.execute("SELECT report FROM workload_reports WHERE report_id = ?", [report_id]).fetchone()
    else:
        row = con.execute("SELECT report FROM workload_reports ORDER BY saved_at DESC, report_id DESC LIMIT 1").fetchone()
    if row is None:
        raise MissingTable("No saved query opportunity report found. Run query-opportunities with --allow-billed first.")
    return json.loads(row[0])


def _decode(key: str, value):
    if key in JSON_FIELDS and isinstance(value, str):
        return json.loads(value)
    return _iso(value)


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


def search(con, q: str = "", limit: int = 50, offset: int = 0, *,
           project: str = "", dataset: str = "") -> dict:
    """A bounded page of literal substring matches and a bounded sample of matching paths."""
    q = text(q, "q", empty=True).strip()
    integer(limit, "limit", 1, SEARCH_LIMIT)
    integer(offset, "offset", 0, 1_000_000)
    text(project, "project", empty=True)
    text(dataset, "dataset", empty=True)
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
    WHERE ($q = '' OR contains(lower(t.fqn), lower($q))
        OR contains(lower(t.description), lower($q)) OR m.total > 0)
      AND ($project = '' OR regexp_replace(t.dataset, '[.][^.]+$', '') = $project)
      AND ($dataset = '' OR t.dataset = $dataset)
    ORDER BY t.num_bytes DESC NULLS LAST, t.fqn
    LIMIT $limit OFFSET $offset
    """, {"q": q, "match_limit": MATCH_LIMIT, "limit": limit + 1, "offset": offset,
          "project": project, "dataset": dataset}).fetchall()
    return {"items": [
        {"table": r[0], "type": r[1], "rows": r[2], "bytes": r[3],
         "full_scan_usd": full_scan_usd(r[1], r[3]), "modified": _iso(r[4]), "description": r[5],
         "matched_columns": r[6] or [], "matched_column_count": r[7], "source": "catalog (free)"}
        for r in rows[:limit]],
        "next_offset": offset + limit if len(rows) > limit else None}


def browse(con) -> dict:
    """Project → dataset hierarchy from current snapshots, including empty datasets.

    Dataset identity is stored as project.dataset throughout the catalog. Group
    on that identity, not table presence or text-search matches, so two projects
    can have identically named datasets and deleted tables cannot revive old ones.
    """
    rows = con.execute(LATEST + """
        SELECT l.dataset, s.taken_at, d.location, d.description,
               count(t.fqn), sum(t.num_bytes), count(*) FILTER (WHERE t.fqn IS NOT NULL AND t.num_bytes IS NULL),
               (SELECT count(*) FROM routines r WHERE r.snapshot_id = l.snapshot_id),
               (SELECT count(*) FROM models m WHERE m.snapshot_id = l.snapshot_id)
        FROM latest l JOIN snapshots s USING (dataset, snapshot_id)
        LEFT JOIN datasets d USING (dataset, snapshot_id)
        LEFT JOIN current_tables t USING (dataset, snapshot_id)
        GROUP BY l.dataset, l.snapshot_id, s.taken_at, d.location, d.description ORDER BY l.dataset
    """).fetchall()
    projects = {r[0]: {"project": r[0], "datasets": [], "tables": 0, "routines": 0, "models": 0}
                for r in con.execute("SELECT project FROM projects ORDER BY project").fetchall()}
    for r in rows:
        project, dataset_id = r[0].rsplit(".", 1)
        node = projects.setdefault(project, {"project": project, "datasets": [], "tables": 0, "routines": 0, "models": 0})
        node["datasets"].append({"dataset": r[0], "dataset_id": dataset_id, "harvested_at": _iso(r[1]),
                                 "location": r[2], "description": r[3], "tables": r[4],
                                 "bytes": r[5] if not r[6] else None, "routines": r[7], "models": r[8]})
        node["tables"] += r[4]
        node["routines"] += r[7]
        node["models"] += r[8]
    return {"projects": [projects[key] for key in sorted(projects)]}


def stats(con) -> dict:
    r = con.execute(LATEST + """
        SELECT (SELECT count(*) FROM latest), count(*), coalesce(sum(num_bytes), 0),
               (SELECT max(taken_at) FROM snapshots JOIN latest USING (dataset, snapshot_id)),
               (SELECT count(*) FROM routines JOIN latest USING (dataset, snapshot_id))
        FROM current_tables
    """).fetchone()
    return {"datasets": r[0], "tables": r[1], "bytes": r[2], "harvested_at": _iso(r[3]), "routines": r[4]}


def _notes(t: dict, columns: list[dict]) -> list[str]:
    """Plain-language hints a person or agent should know before querying this table."""
    notes = []
    if t.get("require_partition_filter"):
        notes.append(f"Queries must filter on the partition column ({t['partitioning']}); BigQuery rejects them otherwise.")
    tagged = [c["path"] for c in columns if c.get("policy_tags")]
    if tagged:
        notes.append(f"{len(tagged)} column(s) have policy tags; reading them may be denied: {', '.join(tagged[:5])}"
                     + (" …" if len(tagged) > 5 else ""))
    if t.get("primary_key") or t.get("foreign_keys"):
        notes.append("Declared keys are not enforced by BigQuery; treat them as join hints, not guarantees.")
    if t.get("type") == "EXTERNAL":
        notes.append("External table: dry runs cannot price reads from the external source.")
    if t.get("expires"):
        notes.append(f"Table expires (is deleted) at {t['expires']}.")
    if t.get("base_table"):
        notes.append(f"{t['base_kind'].title()} of {t['base_table']}.")
    return notes


def describe(con, fqn: str) -> dict:
    text(fqn, "table")
    cur = con.execute(LATEST + " SELECT * FROM current_tables WHERE fqn = ?", [fqn])
    row = cur.fetchone()
    if row is None:
        raise MissingTable("Table is not in the latest catalog snapshot; harvest its dataset first.")
    out = {k: _decode(k, v) for k, v in zip([d[0] for d in cur.description], row)}
    resource = out.pop("resource", None) or {}
    schema_hash = out.pop("schema_hash", None)
    if schema_hash is not None:
        stored = con.execute("SELECT schema FROM schemas WHERE schema_hash=?", [schema_hash]).fetchone()
        if stored is None:
            raise CatalogVersionError("Catalog schema reference is missing; the catalog may be damaged.")
        resource["schema"] = json.loads(stored[0])
    kind, _, field = (out["partitioning"] or "").partition(":")
    out["partition"] = ({"kind": kind, "field": field, "ingestion_time": field == "_PARTITIONTIME",
                         "range": (resource.get("rangePartitioning") or {}).get("range")}
                        if kind else None)
    names = ["path", "type", "mode", "description", "scan_bytes", "policy_tags"]
    cols = con.execute(
        f"SELECT {', '.join(names)} FROM columns WHERE snapshot_id = ? AND fqn = ? ORDER BY rowid",
        [out["snapshot_id"], fqn]).fetchall()
    out["columns"] = [dict(zip(names, c)) for c in cols]
    schema_fields = {}
    def collect_fields(fields, prefix=""):
        for field in fields:
            path = prefix + field["name"]
            schema_fields[path] = {k: v for k, v in field.items()
                                   if k not in {"name", "type", "mode", "description", "policyTags", "fields"}}
            collect_fields(field.get("fields", []), path + ".")
    collect_fields((resource.get("schema") or {}).get("fields", []))
    clusters = (out["clustering"] or "").split(",")
    for column in out["columns"]:
        path = column["path"]
        column["partition_column"] = path == field
        column["clustering_order"] = clusters.index(path) + 1 if path in clusters else None
        column["primary_key"] = path in (out.get("primary_key") or [])
        column["foreign_key"] = any(path == pair[0] for key in out.get("foreign_keys") or [] for pair in key["columns"])
        column["metadata"] = schema_fields.get(path)  # None for snapshots predating full schema retention.
    out["full_scan_usd"] = full_scan_usd(out["type"], out["num_bytes"])
    out["notes"] = _notes(out, out["columns"])
    out["recommendations"] = recommendations(con, out["dataset"].rsplit(".", 1)[0], table=fqn)
    out["source"] = "catalog (tables.get, free)"
    return out


def describe_dataset(con, dataset: str) -> dict:
    """Dataset settings plus its routines, from the latest snapshot."""
    text(dataset, "dataset")
    snap = con.execute(LATEST + " SELECT snapshot_id FROM latest WHERE dataset = ?", [dataset]).fetchone()
    if snap is None:
        raise MissingTable("Dataset is not in the catalog; harvest it first.")
    cur = con.execute("SELECT * EXCLUDE (resource, snapshot_id) FROM datasets WHERE snapshot_id = ?", [snap[0]])
    row = cur.fetchone()
    out = ({k: _decode(k, v) for k, v in zip([d[0] for d in cur.description], row)} if row
           else {"dataset": dataset, "routines_status": None})  # harvested before schema 3
    out["snapshot_id"] = snap[0]
    out["tables"] = con.execute("SELECT count(*) FROM tables WHERE snapshot_id = ?", [snap[0]]).fetchone()[0]
    out["routines"] = [_routine(r, definition_chars=0) for r in con.execute(
        f"SELECT {ROUTINE_COLUMNS} FROM routines WHERE snapshot_id = ? ORDER BY routine_id", [snap[0]]).fetchall()]
    cur = con.execute("SELECT * EXCLUDE (snapshot_id) FROM models WHERE snapshot_id = ? ORDER BY model_id", [snap[0]])
    out["models"] = [{k: _decode(k, v) for k, v in zip([d[0] for d in cur.description], row)} for row in cur.fetchall()]
    out["recommendations"] = recommendations(con, dataset.rsplit(".", 1)[0], dataset=dataset)
    out["source"] = "catalog (datasets.get and routines.get, free)"
    return out


def describe_model(con, fqn: str) -> dict:
    text(fqn, "model")
    cur = con.execute(LATEST + " SELECT m.* FROM models m JOIN latest USING (dataset, snapshot_id) WHERE fqn = ?", [fqn])
    row = cur.fetchone()
    if row is None:
        raise MissingTable("Model is not in the latest catalog snapshot; refresh its dataset first.")
    return {k: _decode(k, v) for k, v in zip([d[0] for d in cur.description], row)}


def recommendations(con, project: str, *, dataset: str = "", table: str = "") -> dict:
    """Read cached regional results only. Unknown or failed coverage is never an empty success."""
    text(project, "project")
    scope_dataset = dataset or (table.rsplit(".", 1)[0] if table else "")
    location = None
    if scope_dataset:
        row = con.execute(LATEST + " SELECT d.location FROM datasets d JOIN latest USING (dataset, snapshot_id) WHERE dataset=?", [scope_dataset]).fetchone()
        location = row[0].lower() if row and row[0] else None
    cur = con.execute("SELECT * FROM recommendation_cache WHERE project = ? ORDER BY location, kind", [project])
    scopes = []
    for row in cur.fetchall():
        entry = {k: _decode(k, v) for k, v in zip([d[0] for d in cur.description], row)}
        if location and entry["location"] != location:
            continue
        fetched = dt.datetime.fromisoformat(entry["fetched_at"]) if entry["fetched_at"] else None
        entry["stale"] = entry["status"] != "harvested" or fetched is None or dt.datetime.now(dt.UTC) - fetched > dt.timedelta(hours=24)
        items = entry["items"] or []
        entry["items"] = [item for item in items if (not table or table in item["tables"])
                          and (not dataset or any(t.rsplit(".", 1)[0] == dataset for t in item["tables"]))]
        scopes.append(entry)
    return {"project": project, "scopes": scopes, "source": "local cache", "cache_hours": 24}


ROUTINE_COLUMNS = "fqn, type, language, arguments, return_type, substr(description, 1, 1000), definition"


def _routine(r, definition_chars: int | None = None) -> dict:
    """definition_chars: None keeps the whole body, 0 drops it, n keeps an n-character excerpt."""
    args = json.loads(r[3]) if r[3] else []
    signature = ", ".join(f"{a.get('name') or ''} {a.get('type') or 'ANY TYPE'}".strip() for a in args)
    out = {"routine": r[0], "type": r[1], "language": r[2], "signature": f"({signature})",
           "returns": r[4], "description": r[5]}
    if definition_chars != 0:
        out["definition"] = r[6] if definition_chars is None or r[6] is None else r[6][:definition_chars]
    return out


def describe_routine(con, fqn: str) -> dict:
    text(fqn, "routine")
    row = con.execute(f"""
        WITH latest AS (SELECT dataset, max(snapshot_id) AS snapshot_id FROM snapshots GROUP BY dataset)
        SELECT {ROUTINE_COLUMNS}, created, modified FROM routines JOIN latest USING (dataset, snapshot_id)
        WHERE fqn = ?""", [fqn]).fetchone()
    if row is None:
        raise MissingTable("Routine is not in the latest catalog snapshot; harvest its dataset first.")
    out = _routine(row)
    out.update(created=_iso(row[7]), modified=_iso(row[8]), source="catalog (routines.get, free)")
    return out


def search_routines(con, q: str = "", limit: int = 50, offset: int = 0) -> dict:
    """Literal substring search over routine names, descriptions and definitions."""
    q = text(q, "q", empty=True).strip()
    integer(limit, "limit", 1, SEARCH_LIMIT)
    integer(offset, "offset", 0, 1_000_000)
    rows = con.execute(f"""
        WITH latest AS (SELECT dataset, max(snapshot_id) AS snapshot_id FROM snapshots GROUP BY dataset)
        SELECT {ROUTINE_COLUMNS} FROM routines JOIN latest USING (dataset, snapshot_id)
        WHERE $q = '' OR contains(lower(fqn), lower($q)) OR contains(lower(description), lower($q))
              OR contains(lower(definition), lower($q))
        ORDER BY fqn LIMIT $limit OFFSET $offset
    """, {"q": q, "limit": limit + 1, "offset": offset}).fetchall()
    # Definitions are excerpts here; describe_routine returns the whole body.
    return {"items": [_routine(r, definition_chars=1000) for r in rows[:limit]],
            "next_offset": offset + limit if len(rows) > limit else None}
