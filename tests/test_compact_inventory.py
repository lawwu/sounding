import json
import shutil
import threading
from unittest.mock import Mock

import duckdb
import pytest
from google.api_core.exceptions import Forbidden, ServiceUnavailable

from sounding import bq, catalog, column_storage, refresh
from conftest import table_row, column_row
from test_bq import fake_client, remote_table, field
from test_schema_dedup import v5_catalog, outputs


def test_column_definitions_shared_but_costs_are_per_table_and_snapshot(seed):
    a, b = table_row(), table_row('p.d.other')
    columns = [column_row(scan_bytes=12), column_row(fqn='p.d.other', scan_bytes=34)]
    seed([a, b], columns)
    seed([a, b], columns)
    with catalog.connect() as con:
        assert con.execute('SELECT count(*) FROM column_schemas').fetchone()[0] == 1
        assert con.execute('SELECT count(*) FROM column_definitions').fetchone()[0] == 1
        assert con.execute('SELECT count(*) FROM column_measurements').fetchone()[0] == 4
        assert con.execute('SELECT count(DISTINCT table_snapshot_id) FROM tables').fetchone()[0] == 4
        assert catalog.describe(con, 'p.d.t')['columns'][0]['scan_bytes'] == 12
        assert catalog.describe(con, 'p.d.other')['columns'][0]['scan_bytes'] == 34
        assert con.execute('SELECT count(*) FROM columns').fetchone()[0] == 4


def test_order_null_empty_and_metadata_differences_are_not_deduplicated(seed):
    for i, rows in enumerate([
        [column_row('a'), column_row('b')], [column_row('b'), column_row('a')],
        [column_row('a', description='changed'), column_row('b')],
        [column_row('a', policy_tags=[]), column_row('b')],
    ]):
        seed(columns=rows)
    with catalog.connect() as con:
        assert con.execute('SELECT count(*) FROM column_schemas').fetchone()[0] == 4
        assert catalog.describe(con, 'p.d.t')['columns'][0]['policy_tags'] == []


def test_column_migration_preserves_every_historical_record_and_rolls_back(tmp_path, monkeypatch):
    source = tmp_path/'source.duckdb'
    v5_catalog(source)
    with duckdb.connect(str(source)) as con:
        con.execute(catalog.MIGRATIONS[5])
        catalog._deduplicate_schemas(con)
        con.execute("UPDATE columns SET scan_bytes=42 WHERE snapshot_id=1")
        before = con.execute('SELECT * FROM columns ORDER BY snapshot_id,fqn,path').fetchall()
        public = outputs(con)
    target = tmp_path/'copy.duckdb'; shutil.copy2(source,target)
    original = column_storage.migrate
    def broken(con):
        original(con)
        raise RuntimeError('interrupted')
    monkeypatch.setattr(column_storage, 'migrate', broken)
    with pytest.raises(RuntimeError):
        with catalog.connect(target): pass
    with duckdb.connect(str(target)) as con:
        assert con.execute('SELECT version FROM catalog_meta').fetchone()[0] == 6
        assert con.execute('SELECT * FROM columns ORDER BY snapshot_id,fqn,path').fetchall() == before
    monkeypatch.setattr(column_storage, 'migrate', original)
    with catalog.connect(target) as con:
        assert con.execute('SELECT * EXCLUDE(rowid) FROM columns ORDER BY snapshot_id,fqn,path').fetchall() == before
        assert outputs(con) == public
    with catalog.connect(target) as con:
        assert outputs(con) == public  # reopen is idempotent


def test_inventory_only_omits_schemas_and_auxiliary_calls_then_loads_details(monkeypatch):
    remote = remote_table('t', [field('id')], type='VIEW', view={'query':'SELECT private_literal'})
    c = fake_client(monkeypatch, [remote])
    c._call_api.return_value = remote.to_api_repr()  # even an oversized response is stripped
    receipt = bq.harvest(['p.d'], inventory_only=True)
    with catalog.connect() as con:
        table = catalog.describe(con, 'p.d.t')
        assert table['metadata_level'] == 'inventory' and table['columns'] == []
        assert table['view_sql'] is None and table['num_bytes'] == 100
        assert con.execute('SELECT count(*) FROM schemas').fetchone()[0] == 0
    c.get_table.assert_not_called(); c.list_routines.assert_not_called(); c.list_models.assert_not_called()
    assert c._call_api.call_args.kwargs['query_params'] == {'fields': bq.INVENTORY_FIELDS}
    assert receipt['metadata_level'] == 'inventory'
    c.get_table.side_effect = lambda _: remote
    result = bq.load_table_metadata('p.d.t')
    assert result['columns'][0]['path'] == 'id' and result['view_sql'] == 'SELECT private_literal'
    assert result['metadata_level'] == 'full'
    c.query.assert_not_called(); c.list_rows.assert_not_called()


def test_hydration_rejects_concurrent_refresh_and_preserves_samples(seed, monkeypatch):
    sid = seed([table_row(preview_rows=[{'id':'keep'}])])
    row, cols = bq.table_record(remote_table().to_api_repr(),'p.d')
    catalog.save_table_metadata('p.d.t', sid, row, cols)
    with catalog.connect() as con:
        assert catalog.describe(con,'p.d.t')['preview_rows'] == [{'id':'keep'}]
    seed()
    with pytest.raises(ValueError, match='catalog changed'):
        catalog.save_table_metadata('p.d.t', sid, row, cols)


def test_filters_receipts_resume_and_transient_retry(monkeypatch):
    discovery, seen = Mock(return_value={'datasets':['p.prod','p.stage','p._hidden'], 'unreachable':[], 'receipt':{}}), []
    monkeypatch.setattr(bq,'project_datasets',discovery)
    def harvest(datasets, progress, **kwargs):
        ds=datasets[0]; seen.append(ds)
        if ds == 'p.prod' and seen.count(ds) == 1: raise ServiceUnavailable('temporary')
        catalog.save_snapshot(ds,[],[])
        return {'coverage':{ds:{'status':'complete','warnings':[]}}}
    monkeypatch.setattr(bq,'harvest',harvest)
    monkeypatch.setattr(refresh.time,'sleep',lambda _:None)
    options=dict(exclude_datasets=['stage*'],inventory_only=True)
    first=refresh.Job(projects=['p'],**options).run()
    assert first['status'] == 'complete' and seen == ['p.prod','p.prod']
    assert first['excluded_datasets'] == ['p._hidden','p.stage']
    assert first['datasets'][0]['receipt']['filters']['exclude_datasets'] == ['stage*']
    resumed=refresh.Job(projects=['p'],resume=True,**options).run()
    assert resumed['status'] == 'complete' and len(seen) == 2
    discovery.assert_called_once()
    assert refresh.Manager().snapshot()['id'] == first['id']
    assert refresh.Job(projects=['p'],resume=True).run()['status'] == 'failed'


def test_failed_datasets_retry_on_resume_completed_ones_do_not(monkeypatch):
    seen=[]
    def harvest(datasets, progress):
        ds=datasets[0]; seen.append(ds)
        if ds == 'p.fail' and seen.count(ds) == 1: raise Forbidden('denied')
        catalog.save_snapshot(ds,[],[])
        return {}
    monkeypatch.setattr(bq,'harvest',harvest)
    first=refresh.Job(['p.ok','p.fail']).run()
    assert first['status'] == 'partial'
    result=refresh.Job(['p.ok','p.fail'],resume=True).run()
    assert result['status'] == 'complete'
    assert seen.count('p.ok') == 1 and seen.count('p.fail') == 2


def test_project_concurrency_is_bounded_and_hidden_is_explicit(monkeypatch):
    barrier=threading.Barrier(2); active=0; peak=0; lock=threading.Lock()
    def discover(project, **kwargs):
        nonlocal active,peak
        assert kwargs == {'include_hidden':True}
        with lock: active+=1; peak=max(peak,active)
        barrier.wait(timeout=3)
        with lock: active-=1
        return {'datasets':[project+'._hidden'], 'unreachable':[], 'receipt':{}}
    monkeypatch.setattr(bq,'project_datasets',discover)
    monkeypatch.setattr(bq,'harvest',lambda *a,**k:{})
    result=refresh.Job(projects=['a','b','c','d'],project_concurrency=2,include_hidden=True).run()
    assert result['status']=='complete' and peak==2 and len(result['datasets'])==4
    with pytest.raises(ValueError): refresh.Job(['p.d'], project_concurrency=5)
    with pytest.raises(ValueError): refresh.Job(['p.d'], inventory_only=True,include_preview_rows=True)


def test_checkpoint_distinguishes_interrupted_from_active_import(monkeypatch):
    from filelock import FileLock
    monkeypatch.setattr(bq,'harvest',lambda *a,**k:{})
    job=refresh.Job(['p.d'])
    job.run()
    saved=json.loads(job.checkpoint.read_text())
    saved['status']='running'
    job.checkpoint.write_text(json.dumps(saved))
    manager=refresh.Manager()
    assert manager.snapshot()['status']=='interrupted'
    with FileLock(str(catalog.CATALOG)+'.refresh.lock'):
        assert manager.snapshot()['status']=='running'
    for invalid in ('', False, {}):
        with pytest.raises(ValueError): refresh.Job(['p.d'],include_datasets=invalid)
