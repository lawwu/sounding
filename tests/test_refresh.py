import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from filelock import FileLock
from google.api_core.exceptions import Forbidden

from sounding import bq, catalog, refresh
from conftest import table_row
from sounding.errors import SoundingError


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


def test_unexpected_failure_is_terminal_in_memory_and_on_disk(monkeypatch):
    def fail(_job):
        raise SoundingError('Refresh could not continue.')
    monkeypatch.setattr(refresh.Job, '_run', fail)
    job = refresh.Job(['p.d'])
    result = job.run()
    assert result['status'] == 'failed' and result['phase'] == 'finished'
    assert result['finished_at'] and result['message'] == 'Refresh could not continue.'
    assert json.loads(job.checkpoint.read_text()) == result
    assert refresh.Manager().snapshot() == result


def test_lock_rejection_and_wrong_resume_do_not_replace_checkpoint(monkeypatch):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    saved = refresh.Job(['p.original'])
    saved.run()
    before = saved.checkpoint.read_bytes()
    with FileLock(str(catalog.CATALOG) + '.refresh.lock'):
        rejected = refresh.Job(['p.other']).run()
    assert rejected['phase'] == 'finished' and rejected['finished_at']
    assert refresh.Job(['p.other'], resume=True).run()['status'] == 'failed'
    assert saved.checkpoint.read_bytes() == before


def test_resume_reenters_discovery_with_unfinished_work_queued(monkeypatch):
    monkeypatch.setattr(bq, 'project_datasets', lambda p: {'datasets':['p.d'], 'unreachable':['EU'], 'receipt':{}})
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {'coverage':{'p.d':{'status':'partial', 'warnings':['Try again.']}}})
    first = refresh.Job(projects=['p']).run()
    observed = []
    manager = refresh.Manager()
    def discover(project):
        observed.append(manager.snapshot())
        return {'datasets':['p.d'], 'unreachable':[], 'receipt':{}}
    monkeypatch.setattr(bq, 'project_datasets', discover)
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    manager.job = refresh.Job(projects=['p'], resume=True)
    result = manager.job.run()
    assert observed[0]['status'] == 'running' and observed[0]['phase'] == 'discovering'
    assert observed[0]['finished_at'] is None
    assert observed[0]['datasets'][0]['status'] == 'queued'
    assert observed[0]['datasets'][0]['message'] is None
    assert 'receipt' not in observed[0]['datasets'][0]
    assert result['id'] == first['id'] and result['started_at'] > first['started_at']
    assert result['status'] == 'complete'


def test_completed_job_cannot_be_run_twice(monkeypatch):
    harvest = Mock(return_value={})
    monkeypatch.setattr(bq, 'harvest', harvest)
    job = refresh.Job(['p.d'])
    completed = job.run()
    with pytest.raises(SoundingError, match='already started'):
        job.run()
    assert job.snapshot() == completed
    harvest.assert_called_once()


def test_manager_follows_later_external_refresh_and_resume(monkeypatch):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    manager = refresh.Manager()
    manager.job = refresh.Job(['p.old'])
    manager.job.run()
    external = refresh.Job(['p.new']).run()
    assert manager.snapshot() == external
    resumed = refresh.Job(['p.new'], resume=True).run()
    assert manager.snapshot() == resumed


def test_manager_follows_owner_after_its_own_start_is_rejected(monkeypatch):
    started, release = threading.Event(), threading.Event()
    def harvest(*a, **k):
        started.set()
        assert release.wait(5)
        return {}
    monkeypatch.setattr(bq, 'harvest', harvest)
    owner = refresh.Job(['p.owner'])
    thread = threading.Thread(target=owner.run)
    thread.start()
    try:
        assert started.wait(5)
        manager = refresh.Manager()
        manager.job = refresh.Job(['p.rejected'])
        assert manager.job.run()['status'] == 'failed'
        assert manager.snapshot()['id'] == owner.snapshot()['id']
        assert manager.snapshot()['status'] == 'running'
    finally:
        release.set()
        thread.join(5)
    observed = manager.snapshot()
    assert 'Another metadata refresh' in observed.pop('submission_error')
    assert observed == owner.snapshot()


def test_finished_checkpoint_is_reread_before_reporting_interruption(monkeypatch):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    job = refresh.Job(['p.d'])
    completed = job.run()
    read = refresh._read_checkpoint
    calls = 0
    def finishing(path):
        nonlocal calls
        calls += 1
        state = read(path)
        if calls == 1:
            state.update(status='running', phase='harvesting', finished_at=None)
        return state
    monkeypatch.setattr(refresh, '_read_checkpoint', finishing)
    assert refresh.Manager().snapshot() == completed


@pytest.mark.parametrize('contents', ['{', '[]', '{}', '{"status":[]}'])
def test_invalid_checkpoint_is_actionable_and_never_resumed(monkeypatch, contents):
    job = refresh.Job(['p.d'], resume=True)
    job.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    job.checkpoint.write_text(contents)
    harvest = Mock()
    monkeypatch.setattr(bq, 'harvest', harvest)
    state = refresh.Manager().snapshot()
    assert state['status'] == 'interrupted' and 'new refresh' in state['message']
    assert job.run()['status'] == 'failed'
    assert job.checkpoint.read_text() == contents
    harvest.assert_not_called()


def test_final_checkpoint_failure_does_not_relabel_published_metadata(monkeypatch):
    job = refresh.Job(['p.d'])
    def checkpoint():
        if job.snapshot()['status'] == 'complete':
            raise OSError('disk full')
    monkeypatch.setattr(job, '_checkpoint', checkpoint)
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    result = job.run()
    assert result['status'] == 'complete'
    assert any('checkpoint may be out of date' in w for w in result['warnings'])


def test_interruption_is_saved_before_releasing_refresh_ownership(monkeypatch):
    def interrupt(_job):
        raise KeyboardInterrupt
    monkeypatch.setattr(refresh.Job, '_run', interrupt)
    job = refresh.Job(['p.d'])
    with pytest.raises(KeyboardInterrupt):
        job.run()
    state = refresh.Manager().snapshot()
    assert state['status'] == 'interrupted' and state['phase'] == 'finished'
    assert state['finished_at']
    with FileLock(str(catalog.CATALOG) + '.refresh.lock', timeout=0):
        assert json.loads(job.checkpoint.read_text()) == state


def test_new_refresh_waits_for_terminal_checkpoint_publication(monkeypatch):
    saving, release = threading.Event(), threading.Event()
    checkpoint = refresh.Job._checkpoint
    def save(job):
        if job.snapshot()['status'] == 'complete':
            saving.set()
            assert release.wait(5)
        checkpoint(job)
    monkeypatch.setattr(refresh.Job, '_checkpoint', save)
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    manager = refresh.Manager()
    manager.start(['p.d'])
    try:
        assert saving.wait(5)
        with pytest.raises(SoundingError, match='already running'):
            manager.start(['p.other'])
    finally:
        release.set()
        assert manager.job.finished.wait(5)
    assert manager.snapshot()['status'] == 'complete'


def test_rejected_start_cannot_hide_interrupted_external_owner(monkeypatch):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    owner = refresh.Job(['p.owner'])
    saved = owner.run()
    saved.update(status='running', phase='harvesting', finished_at=None)
    saved['datasets'][0]['status'] = 'running'
    owner.checkpoint.write_text(json.dumps(saved))
    manager = refresh.Manager()
    manager.job = refresh.Job(['p.rejected'])
    with FileLock(str(catalog.CATALOG) + '.refresh.lock'):
        assert manager.job.run()['status'] == 'failed'
        assert manager.snapshot()['id'] == saved['id']
    # Simulate the external owner exiting without a terminal checkpoint.
    observed = manager.snapshot()
    assert observed['id'] == saved['id'] and observed['status'] == 'interrupted'
    assert observed['requested_datasets'] == ['p.owner']
    assert observed['datasets'][0]['status'] == 'interrupted'
    resumed = refresh.Job(observed['requested_datasets'], resume=True).run()
    assert resumed['id'] == saved['id'] and resumed['status'] == 'complete'


@pytest.mark.parametrize('during_reread', [False, True])
def test_unreadable_checkpoint_is_not_hidden_by_cached_local_success(monkeypatch, during_reread):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    manager = refresh.Manager()
    manager.job = refresh.Job(['p.d'])
    completed = manager.job.run()
    if during_reread:
        read = refresh._read_checkpoint
        calls = 0
        def corrupt_on_reread(path):
            nonlocal calls
            calls += 1
            if calls == 1:
                return completed | {'status':'running', 'finished_at':None}
            path.write_text('{')
            return read(path)
        monkeypatch.setattr(refresh, '_read_checkpoint', corrupt_on_reread)
    else:
        manager.job.checkpoint.write_text('{')
    observed = manager.snapshot()
    assert observed['status'] == 'interrupted'
    assert 'new refresh' in observed['message']
    assert 'id' not in observed  # No stale import scope offered for resume.


def test_rejected_resume_preserves_saved_targets_and_reports_submission_error(monkeypatch):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    saved = refresh.Job(['p.latest']).run()
    manager = refresh.Manager()
    manager.job = refresh.Job(['p.stale'], resume=True)
    assert manager.job.run()['status'] == 'failed'
    observed = manager.snapshot()
    assert observed['id'] == saved['id'] and observed['requested_datasets'] == ['p.latest']
    assert 'Resume requires the same' in observed['submission_error']
    assert refresh.Manager().snapshot() == saved  # Rejection is not persisted over the saved import.
    manager.job = refresh.Job(observed['requested_datasets'], resume=True)
    assert manager.job.run()['status'] == 'complete'
    assert 'submission_error' not in manager.snapshot()


@pytest.mark.parametrize('interrupted', [False, True])
def test_checkpoint_authority_does_not_depend_on_wall_clock_order(monkeypatch, interrupted):
    monkeypatch.setattr(bq, 'harvest', lambda *a, **k: {})
    manager = refresh.Manager()
    manager.job = refresh.Job(['p.local'])
    manager.job.run()
    external = refresh.Job(['p.external'])
    saved = external.run()
    saved.update(started_at='2020-01-01T00:00:00+00:00', finished_at='2020-01-01T00:01:00+00:00')
    if interrupted:
        saved.update(status='running', phase='harvesting', finished_at=None)
    external.checkpoint.write_text(json.dumps(saved))
    observed = manager.snapshot()
    assert observed['id'] == saved['id']
    assert observed['requested_datasets'] == ['p.external']
    assert observed['status'] == ('interrupted' if interrupted else 'complete')
