"""sounding.source: BigQuery reads for other Python programs, with no local catalog involved."""
import datetime as dt
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from sounding import catalog, source
from sounding.errors import SoundingError

UTC = dt.UTC
SINCE, UNTIL = dt.datetime(2026, 9, 2, tzinfo=UTC), dt.datetime(2026, 9, 3, tzinfo=UTC)


def job(name, *, statement='SELECT', reads=('raw.shift_totals',), writes=None, error=None, sql='SELECT 7 FROM t WHERE employee = 42',
        parent=None, children=0, **query):
    query = {'statementType': statement, 'totalBytesProcessed': '2048', 'totalBytesBilled': '10485760', 'totalSlotMs': '1234',
             'referencedTables': [dict(zip(('projectId', 'datasetId', 'tableId'), ('data-p', *t.split('.')))) for t in reads]} | query
    config = {'query': sql}
    if writes:
        config['destinationTable'] = dict(zip(('projectId', 'datasetId', 'tableId'), ('data-p', *writes.split('.'))))
    stats = {'creationTime': '1788314320000', 'startTime': '1788314321000', 'endTime': '1788314392000', 'query': query}
    if parent:
        stats['parentJobId'] = parent
    if children:
        stats['numChildJobs'] = str(children)
    return bigquery.QueryJob.from_api_repr({
        'jobReference': {'projectId': 'jobs-p', 'jobId': name, 'location': 'us-central1'},
        'configuration': {'query': config, 'labels': {'team': 'workforce'}}, 'statistics': stats,
        'status': {'state': 'DONE'} | ({'errorResult': {'reason': error, 'message': 'employee 42 not found'}} if error else {})}, Mock())


def client_for(pages, children=None, unreachable=()):
    class Page(list):
        _raw_page = {'unreachable': list(unreachable)}

    def list_jobs(**kwargs):
        if kwargs.get('parent_job'):
            return NS(pages=iter([Page((children or {})[kwargs['parent_job']])]))
        return NS(pages=iter([Page(p) for p in pages]))
    return Mock(project='default-p', list_jobs=Mock(side_effect=list_jobs))


def test_a_table_is_read_with_nested_columns_and_its_snapshot_base_without_touching_the_catalog():
    table = bigquery.Table('data-p.frozen.shift_inputs_b042', schema=[
        bigquery.SchemaField('employee_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('shift', 'RECORD', fields=[bigquery.SchemaField('hours_at_cutoff', 'FLOAT', description='Hours at the cutoff')])])
    table._properties |= {'type': 'SNAPSHOT', 'location': 'us-central1', 'creationTime': '1788314520000', 'snapshotDefinition': {
        'baseTableReference': {'projectId': 'data-p', 'datasetId': 'features', 'tableId': 'shift_inputs'}, 'snapshotTime': '2026-09-02T02:00:00Z'}}
    client = Mock(get_table=Mock(return_value=table))

    result = source.table('data-p.frozen.shift_inputs_b042', client=client)

    assert result['table']['fqn'] == 'data-p.frozen.shift_inputs_b042' and result['table']['dataset'] == 'data-p.frozen'
    assert result['table']['base_table'] == 'data-p.features.shift_inputs' and result['table']['base_kind'] == 'SNAPSHOT'
    assert result['table']['resource']['snapshotDefinition']['snapshotTime'] == '2026-09-02T02:00:00Z'
    assert [(c['path'], c['type'], c['mode']) for c in result['columns']] == [
        ('employee_id', 'STRING', 'REQUIRED'), ('shift', 'RECORD', 'NULLABLE'), ('shift.hours_at_cutoff', 'FLOAT', 'NULLABLE')]
    assert result['columns'][2]['description'] == 'Hours at the cutoff'
    assert result['receipt']['free'] and not catalog.CATALOG.exists()
    client.query.assert_not_called()


def test_every_query_job_in_a_window_is_reported_once_with_what_it_read_and_wrote():
    script = job('script-1', statement='SCRIPT', reads=(), children=1)
    client = client_for(
        pages=[[job('ctas', statement='CREATE_TABLE_AS_SELECT', writes='features.shift_inputs'), script],
               [job('cached', reads=(), writes='_a1b2c3.anon9f', cacheHit=True), job('failed', reads=(), error='invalidQuery')]],
        children={'script-1': [job('script-1_0', statement='INSERT', writes='features.audit', parent='script-1')]}, unreachable=['EU'])

    pages = list(source.job_pages('jobs-p', SINCE, UNTIL, client=client))

    assert [[j['job_id'] for j in p['jobs']] for p in pages] == [['ctas', 'script-1', 'script-1_0'], ['cached', 'failed']]
    assert pages[0]['unreachable'] == ['EU'] and pages[0]['receipt']['free']
    ctas, parent, child = pages[0]['jobs']
    assert ctas == {'project': 'jobs-p', 'location': 'us-central1', 'job_id': 'ctas', 'parent_job_id': None, 'child_jobs': 0,
                    'created': '2026-09-02T01:58:40+00:00', 'started': '2026-09-02T01:58:41+00:00', 'ended': '2026-09-02T01:59:52+00:00',
                    'state': 'DONE', 'statement_type': 'CREATE_TABLE_AS_SELECT', 'error_reason': None, 'cache_hit': None,
                    'processed_bytes': 2048, 'billed_bytes': 10485760, 'slot_ms': 1234,
                    'referenced_tables': ['data-p.raw.shift_totals'], 'destination_table': 'data-p.features.shift_inputs',
                    'destination_temporary': False, 'labels': {'team': 'workforce'}}
    assert parent['child_jobs'] == 1 and (child['parent_job_id'], child['destination_table']) == ('script-1', 'data-p.features.audit')
    cached, failed = pages[1]['jobs']
    assert cached['cache_hit'] is True and cached['destination_temporary'] and cached['referenced_tables'] == []
    assert failed['error_reason'] == 'invalidQuery' and failed['state'] == 'DONE'
    assert '42' not in str(pages)  # neither SQL nor error messages unless asked for

    top = client.list_jobs.call_args_list[0].kwargs
    assert (top['project'], top['all_users'], top['min_creation_time'], top['max_creation_time']) == ('jobs-p', True, SINCE, UNTIL)
    assert client.list_jobs.call_args_list[1].kwargs['parent_job'] == 'script-1'
    client.query.assert_not_called()


def test_sql_text_is_returned_only_when_asked_for_and_other_job_types_are_counted():
    load = bigquery.LoadJob.from_api_repr({'jobReference': {'projectId': 'jobs-p', 'jobId': 'load-1'}, 'configuration': {'load': {}}}, Mock())
    page, = source.job_pages('jobs-p', SINCE, UNTIL, client=client_for([[job('q'), load]]), include_sql=True, all_users=False)
    assert [j['sql'] for j in page['jobs']] == ['SELECT 7 FROM t WHERE employee = 42'] and page['other_jobs'] == 1


@pytest.mark.parametrize('args', [('jobs-p', UNTIL, SINCE), ('jobs-p', SINCE.replace(tzinfo=None), UNTIL), ('bad project!', SINCE, UNTIL),
                                  ('jobs-p', SINCE - dt.timedelta(days=400), UNTIL)])
def test_a_window_is_checked_before_any_call(args):
    client = Mock()
    with pytest.raises(SoundingError):
        list(source.job_pages(*args, client=client))
    client.list_jobs.assert_not_called()


def test_the_readers_load_without_the_catalog_engine():
    code = "import sys, sounding.source; sys.exit(any(m in sys.modules for m in ('duckdb', 'sounding.catalog')))"
    assert subprocess.run([sys.executable, '-c', code]).returncode == 0
