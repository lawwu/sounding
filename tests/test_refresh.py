import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from filelock import FileLock
from google.api_core.exceptions import Forbidden

from sounding import bq, catalog, refresh
from conftest import table_row


def test_project_refresh_discovers_new_datasets_deduplicates_and_keeps_failures(seed, monkeypatch):
    old = seed([table_row('p.denied.old')], [], dataset='p.denied')
    seen = []
    def discovery(project):
        if project == 'blocked':
            raise Forbidden('secret account details')
        return {'datasets':['p.new', 'p.denied'], 'unreachable':[], 'receipt':bq.free_api_receipt('datasets.list')}
    def harvest(datasets, progress):
        ds = datasets[0]
        seen.append(ds)
        progress.add(2)
        progress.advance()
        if ds.endswith('denied'):
            raise Forbidden('secret account details')
        catalog.save_snapshot(ds, [], [])
        progress.advance()
        return bq.free_api_receipt('tables.get')
    monkeypatch.setattr(bq, 'project_datasets', discovery)
    monkeypatch.setattr(bq, 'harvest', harvest)
    result = refresh.Job(['p.new'], ['p', 'blocked']).run()
    assert sorted(seen) == ['p.denied', 'p.new']
    assert result['status'] == 'partial'
    assert result['progress'] == {'datasets_done':2,'datasets_total':2,'done':3,'total':4}
    assert 'secret account' not in str(result)
    with catalog.connect() as con:
        assert catalog.describe(con, 'p.denied.old')['snapshot_id'] == old
        assert catalog.describe_dataset(con, 'p.new')['tables'] == 0


def test_background_progress_reads_and_duplicate_refresh_rejection(monkeypatch):
    started, release = threading.Event(), threading.Event()
    def harvest(datasets, progress):
        progress.add(3); progress.advance()
        started.set()
        assert release.wait(5)
        progress.advance(2)
        return bq.free_api_receipt('tables.get')
    monkeypatch.setattr(bq, 'harvest', harvest)
    manager = refresh.Manager()
    manager.start(['p.d'])
    try:
        assert started.wait(5)
        state = manager.snapshot()
        assert state['status'] == 'running' and state['progress']['done'] == 1
        with catalog.connect() as con:
            assert catalog.stats(con)['tables'] == 0
        with pytest.raises(ValueError, match='already running'):
            manager.start(['p.other'])
        state['datasets'][0]['status'] = 'tampered'
        assert manager.snapshot()['datasets'][0]['status'] == 'running'
    finally:
        release.set()
        # Wait for the background worker before its isolated catalog is removed.
        for thread in threading.enumerate():
            if thread.name.endswith('(run)'):
                thread.join(timeout=5)
    assert manager.snapshot()['status'] == 'complete'


def test_other_process_lock_blocks_without_network(monkeypatch):
    with FileLock(str(catalog.CATALOG) + '.refresh.lock'):
        result = refresh.Job(['p.d']).run()
    assert result['status'] == 'failed'
    assert 'Another metadata refresh' in result['message']


@pytest.mark.parametrize('datasets,projects', [([],[]), ('p.d',[]), ([None],[]), (['p.d;bad'],[]), ([],['bad/project']), (['d'],[])])
def test_invalid_targets_fail_before_start(datasets, projects):
    with pytest.raises(ValueError):
        refresh.Job(datasets, projects)


def test_project_discovery_paginates_and_reports_unreachable_locations(monkeypatch):
    class Page(list):
        def __init__(self, ids, unreachable=()):
            super().__init__(NS(project='p', dataset_id=d) for d in ids)
            self._raw_page = {'unreachable': list(unreachable)}
    client = Mock()
    client.list_datasets.return_value = NS(pages=iter([Page(['one']), Page(['two'], ['EU'])]))
    monkeypatch.setattr(bq, 'client', lambda:client)
    result = bq.project_datasets('p')
    assert result['datasets'] == ['p.one','p.two']
    assert result['unreachable'] == ['EU']
    client.query.assert_not_called()
    client.close.assert_called_once()
