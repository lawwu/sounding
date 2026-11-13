import datetime as dt
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from sounding import bq


def job(name, table='t', billed='100', **query):
    query = {'statementType':'SELECT', 'referencedTables':[{'projectId':'p','datasetId':'d','tableId':table}],
             'totalSlotMs':'1234', 'totalBytesBilled':billed} | query
    if billed is None:
        del query['totalBytesBilled']
    return bigquery.QueryJob.from_api_repr({'jobReference':{'projectId':'jobs-p','jobId':name,'location':'US'},
        'configuration':{'query':{'query':'SELECT secret FROM `p.d.t`'}},
        'statistics':{'creationTime':'1794440000000','query':query},'status':{'state':'DONE'}}, Mock())


def client_for(monkeypatch, jobs, unreachable=()):
    class Page(list):
        _raw_page = {'unreachable':list(unreachable)}
    client = Mock(project='default-jobs')
    client.list_jobs.return_value = NS(pages=iter([Page(jobs)]))
    monkeypatch.setattr(bq, 'client', lambda:client)
    return client


def test_table_history_ranks_exact_references_and_does_not_return_sql(monkeypatch):
    failed = job('failed', billed='999999')
    failed._properties['status']['errorResult'] = {'reason':'invalidQuery'}
    c = client_for(monkeypatch, [job('unknown', billed=None), job('other', table='other'),
                               job('small'), job('big', billed='500'), job('script', statementType='SCRIPT'), failed])
    result = bq.table_queries('p.d.t', project='jobs-p', scope='project')
    assert [j['job_id'] for j in result['jobs']] == ['big','small','unknown']
    assert result['jobs'][-1]['billed_bytes'] is None
    assert result['days'] == 30 and result['scripts_excluded'] == 1
    args = c.list_jobs.call_args.kwargs
    assert args['project'] == 'jobs-p' and args['all_users'] is True
    assert args['max_creation_time'] - args['min_creation_time'] == dt.timedelta(days=30)
    assert args['state_filter'] == 'done'
    assert 'secret' not in str(result)
    c.query.assert_not_called(); c.get_job.assert_not_called()


def test_history_exposes_scan_limit_missing_references_and_slot_ranking(monkeypatch):
    monkeypatch.setattr(bq, 'COST_REPORT_MAX_JOBS', 3)
    c = client_for(monkeypatch, [job('cached', referencedTables=[], cacheHit=True),
        job('low', totalSlotMs='2'), job('high', billed='0', totalSlotMs='9000'), job('outside-cap', totalSlotMs='99999')], ['EU'])
    result = bq.table_queries('p.d.t', days=1, rank_by='slot_ms', limit=1)
    assert result['truncated'] and result['results_limited']
    assert result['missing_references'] == 1 and result['jobs_scanned'] == 3
    assert result['jobs'][0]['job_id'] == 'high'
    assert any('EU' in w for w in result['warnings'])
    assert c.list_jobs.call_args.kwargs['all_users'] is False


@pytest.mark.parametrize('kwargs', [{'days':0},{'days':181},{'days':True},{'scope':'everyone'},{'rank_by':'cost'},{'limit':101}])
def test_history_validates_before_network(kwargs):
    with pytest.raises(ValueError):
        bq.table_queries('p.d.t', **kwargs)
