import hashlib
import json
import shutil

import duckdb
import pyarrow as pa
import pytest

from sounding import bq, catalog
from test_bq import fake_client, remote_table, field


def outputs(con):
    tables = catalog.search(con, limit=100)
    datasets = con.execute('SELECT DISTINCT dataset FROM snapshots ORDER BY dataset').fetchall()
    return json.dumps({"stats": catalog.stats(con), "browse": catalog.browse(con), "search": tables,
        "tables": [catalog.describe(con, t["table"]) for t in tables["items"]],
        "datasets": [catalog.describe_dataset(con, d[0]) for d in datasets]})


def test_harvest_shares_schema_across_tables_and_snapshots(monkeypatch):
    fields = [field("nested", "RECORD", [field("x", precision="12", scale="2")]), field("id")]
    c = fake_client(monkeypatch, [remote_table("a", fields), remote_table("b", fields)])
    bq.harvest(["p.d"])
    with catalog.connect() as con:
        before = outputs(con)
    bq.harvest(["p.d"])
    with catalog.connect() as con:
        assert con.execute('SELECT count(*) FROM schemas').fetchone()[0] == 1
        assert con.execute('SELECT count(*) FROM tables').fetchone()[0] == 4
        assert con.execute("SELECT count(*) FROM tables WHERE json_exists(resource, '$.schema')").fetchone()[0] == 0
        assert catalog.describe(con, 'p.d.a')['columns'][1]['metadata']['precision'] == '12'
    assert 'schema_hash' not in before


def test_canonical_keys_preserve_field_order_and_distinct_metadata():
    first = {"fields": [{"name":"a", "type":"STRING"}, {"name":"b", "type":"INT64"}]}
    reordered_keys = {"fields": [{"type":"STRING", "name":"a"}, {"type":"INT64", "name":"b"}]}
    row, canonical = catalog._extract_schema({"resource":{"schema":first}})
    assert row['schema_hash'] == hashlib.sha256(json.dumps(first, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert catalog._extract_schema({"resource":{"schema":reordered_keys}})[0]['schema_hash'] == row['schema_hash']
    assert catalog._extract_schema({"resource":{"schema":{"fields":list(reversed(first['fields']))}}})[0]['schema_hash'] != row['schema_hash']
    assert first['fields'][0]['name'] == 'a'  # input is never mutated


def v5_catalog(path):
    with duckdb.connect(str(path)) as con:
        con.execute(catalog.SCHEMA_V2)
        for step in range(2,5):
            con.execute(catalog.MIGRATIONS[step])
        for sid in (1,2):
            con.execute("INSERT INTO snapshots VALUES (?, '2026-01-01', 'p.d')", [sid])
            for name, fields in [('a',[field('a', maxLength='42')]), ('b',[field('a', maxLength='42')]), ('different',[field('a', maxLength='43')])]:
                row, columns = bq.table_record(remote_table(name, fields).to_api_repr(), 'p.d')
                row.update(snapshot_id=sid, preview_rows=[{'a':'keep me'}], preview_status='harvested')
                con.register('batch',pa.Table.from_pylist([catalog._encode(row)], schema=pa.schema([f for f in catalog.TABLE_SCHEMA if f.name not in ('schema_hash', 'metadata_level')])))
                con.execute('INSERT INTO tables BY NAME SELECT * FROM batch')
                con.register('batch',pa.Table.from_pylist([dict(c,snapshot_id=sid) for c in columns],schema=catalog.COLUMN_SCHEMA))
                con.execute('INSERT INTO columns BY NAME SELECT * FROM batch')
        for name, resource in [('absent',{}), ('null',{'schema':None}), ('none',None), ('empty',{'schema':{}})]:
            con.execute('INSERT INTO tables(snapshot_id,dataset,fqn,resource) VALUES (2,\'p.d\',?,?)', ['p.d.'+name,json.dumps(resource) if resource is not None else None])
        return outputs(con)


def test_v5_migration_lossless_batched_and_repeatable(tmp_path, monkeypatch, capsys):
    original, migrated = tmp_path/'v5.duckdb', tmp_path/'copy.duckdb'
    before = v5_catalog(original)
    shutil.copy2(original,migrated)
    migrate = catalog._deduplicate_schemas
    monkeypatch.setattr(catalog, '_deduplicate_schemas', lambda con, **kwargs: migrate(con, batch_size=2))
    with catalog.connect(migrated) as con:
        assert outputs(con) == before
        assert con.execute('SELECT count(*) FROM schemas').fetchone()[0] == 3
        assert con.execute('SELECT count(*) FROM tables').fetchone()[0] == 10
        assert con.execute('SELECT count(*) FROM snapshots').fetchone()[0] == 2
    with catalog.connect(migrated,write=True) as con:
        con.execute('BEGIN')
        catalog._deduplicate_schemas(con,batch_size=1)
        con.execute('COMMIT')
        assert outputs(con) == before
        assert con.execute('SELECT count(*) FROM schemas').fetchone()[0] == 3
    assert 'existing data and snapshots are unchanged' in capsys.readouterr().err
    with duckdb.connect(str(original),read_only=True) as con:
        assert con.execute('SELECT version FROM catalog_meta').fetchone()[0] == 5
        cur = con.execute('SELECT * FROM tables ORDER BY snapshot_id, fqn')
        names = [d[0] for d in cur.description]
        originals = [dict(zip(names, row)) for row in cur.fetchall()]
    with catalog.connect(migrated) as con:
        cur = con.execute('SELECT * FROM tables ORDER BY snapshot_id, fqn')
        names = [d[0] for d in cur.description]
        restored = [dict(zip(names, row)) for row in cur.fetchall()]
        for row in restored:
            for key in ('table_snapshot_id', 'column_schema_id', 'metadata_level'):
                row.pop(key, None)
            schema_hash = row.pop('schema_hash')
            if schema_hash:
                resource = json.loads(row['resource'])
                resource['schema'] = json.loads(con.execute('SELECT schema FROM schemas WHERE schema_hash=?',[schema_hash]).fetchone()[0])
                row['resource'] = json.dumps(resource, sort_keys=True)
        assert restored == originals  # all historic fields, including preview_rows


def test_failed_schema_migration_rolls_back(tmp_path, monkeypatch):
    path=tmp_path/'v5.duckdb'
    before=v5_catalog(path)
    migrate=catalog._deduplicate_schemas
    def fail(con):
        migrate(con, batch_size=1)
        raise RuntimeError('interrupted')
    monkeypatch.setattr(catalog,'_deduplicate_schemas',fail)
    with pytest.raises(RuntimeError):
        with catalog.connect(path):
            pass
    with duckdb.connect(str(path)) as con:
        assert con.execute('SELECT version FROM catalog_meta').fetchone()[0] == 5
        assert outputs(con) == before
