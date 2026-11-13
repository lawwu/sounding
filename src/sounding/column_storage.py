"""Shared flattened schemas and sparse, table-specific column measurements."""
from contextlib import suppress
import duckdb
import hashlib
import json

import pyarrow as pa

DDL = """
CREATE SEQUENCE table_snapshot_ids;
ALTER TABLE tables ADD COLUMN table_snapshot_id BIGINT;
ALTER TABLE tables ALTER COLUMN table_snapshot_id SET DEFAULT nextval('table_snapshot_ids');
CREATE UNIQUE INDEX table_snapshot_identity ON tables(table_snapshot_id);
UPDATE tables SET table_snapshot_id=nextval('table_snapshot_ids');
ALTER TABLE tables ADD COLUMN column_schema_id BIGINT;
CREATE SEQUENCE column_schema_ids;
CREATE TABLE column_schemas (
    column_schema_id BIGINT PRIMARY KEY DEFAULT nextval('column_schema_ids'),
    fingerprint VARCHAR UNIQUE NOT NULL);
CREATE TABLE column_definitions (
    column_schema_id BIGINT, path VARCHAR, ordinal BIGINT NOT NULL,
    type VARCHAR, mode VARCHAR, description VARCHAR, policy_tags VARCHAR[],
    PRIMARY KEY(column_schema_id, path));
CREATE TABLE column_measurements (
    table_snapshot_id BIGINT, path VARCHAR, scan_bytes BIGINT NOT NULL,
    PRIMARY KEY(table_snapshot_id, path));
"""
VIEW = """
CREATE VIEW columns AS
SELECT t.snapshot_id, t.fqn, d.path, d.type, d.mode, d.description, m.scan_bytes,
       d.policy_tags, d.ordinal AS rowid
FROM tables t JOIN column_definitions d USING(column_schema_id)
LEFT JOIN column_measurements m ON m.table_snapshot_id=t.table_snapshot_id AND m.path=d.path;
"""
FIELDS = ('path', 'type', 'mode', 'description', 'policy_tags')


def store(con, table_id, rows):
    """Preserve field order and attributes; measurements never participate in sharing."""
    if not rows:
        return
    definitions = [{k: r.get(k) for k in FIELDS} for r in rows]
    canonical = json.dumps(definitions, sort_keys=True, separators=(',', ':'))
    fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
    existing = con.execute('SELECT column_schema_id FROM column_schemas WHERE fingerprint=?', [fingerprint]).fetchone()
    if existing:
        schema_id = existing[0]
        # Still reject duplicate paths even when a caller provides an existing schema.
    else:
        schema_id = con.execute('INSERT INTO column_schemas(fingerprint) VALUES (?) RETURNING column_schema_id', [fingerprint]).fetchone()[0]
        schema = pa.schema([('column_schema_id', pa.int64()), ('path', pa.string()), ('ordinal', pa.int64()),
                            ('type', pa.string()), ('mode', pa.string()), ('description', pa.string()),
                            ('policy_tags', pa.list_(pa.string()))])
        con.register('definition_batch', pa.Table.from_pylist([
            dict(r, column_schema_id=schema_id, ordinal=i) for i, r in enumerate(definitions)], schema=schema))
        try:
            con.execute('INSERT INTO column_definitions BY NAME SELECT * FROM definition_batch')
        finally:
            with suppress(duckdb.TransactionException):
                con.unregister('definition_batch')
    con.execute('UPDATE tables SET column_schema_id=? WHERE table_snapshot_id=?', [schema_id, table_id])
    measured = [(table_id, r['path'], r['scan_bytes']) for r in rows if r.get('scan_bytes') is not None]
    if measured:
        con.executemany('INSERT INTO column_measurements VALUES (?, ?, ?)', measured)


def migrate(con):
    """Caller owns the transaction. Memory is bounded to one table's flattened schema."""
    if con.execute('''SELECT count(*) FROM columns c LEFT JOIN tables t USING(snapshot_id,fqn)
                      WHERE t.fqn IS NULL''').fetchone()[0]:
        raise ValueError('Cannot migrate orphan column records; original catalog retained.')
    con.execute(DDL)
    last = 0
    while True:
        tables = con.execute('''SELECT table_snapshot_id,snapshot_id,fqn FROM tables
                               WHERE table_snapshot_id>? ORDER BY table_snapshot_id LIMIT 256''', [last]).fetchall()
        if not tables:
            break
        for table_id, sid, fqn in tables:
            cursor = con.execute('SELECT path,type,mode,description,policy_tags,scan_bytes FROM columns WHERE snapshot_id=? AND fqn=? ORDER BY rowid', [sid,fqn])
            names = [d[0] for d in cursor.description]
            store(con, table_id, [dict(zip(names,r)) for r in cursor.fetchall()])
        last = tables[-1][0]
    con.execute('DROP TABLE columns')
    con.execute(VIEW)
