import errno
import http.client
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from sounding import bq, catalog, ui
from conftest import column_row, table_row


@pytest.fixture
def server(seed):
    seed([table_row("p.d.orders")], [column_row("hits.page.path", "p.d.orders")])
    srv = ui.bind(0)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()
    thread.join(timeout=5)


def request(server, path, method="GET", body=None, headers=None, *, authorized=True):
    defaults = {}
    if authorized:
        defaults["X-Sounding-Token"] = server.api_token
    if method == "POST":
        defaults.update({"Origin": f"http://127.0.0.1:{server.server_port}", "Content-Type":"application/json"})
    defaults.update(headers or {})
    con = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    con.request(method, path, body=body, headers=defaults)
    response = con.getresponse()
    result = response.status, dict(response.headers), response.read()
    con.close()
    return result


def test_page_script_and_catalog_endpoints(server):
    status, headers, body = request(server, "/", authorized=False)
    assert status == 200 and b"<title>Sounding</title>" in body
    assert server.api_token.encode() in body and b"__SOUNDING_TOKEN__" not in body
    assert "frame-ancestors 'none'" in headers["Content-Security-Policy"]
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert request(server,"/ui.js",authorized=False)[0] == 200
    stats = json.loads(request(server,"/api/stats")[2])
    assert stats["tables"] == 1 and stats["datasets"] == 1
    page = json.loads(request(server,"/api/search?q=page")[2])
    assert page["items"][0]["matched_columns"] == ["hits.page.path"]
    assert json.loads(request(server,"/api/table?fqn=p.d.orders")[2])["created"].endswith("+00:00")


@pytest.mark.parametrize("host",["attacker.invalid", "127.0.0.1", "127.0.0.1:1", "localhost.attacker.invalid:8765", "127.0.0.1:8765@attacker.invalid"])
def test_rebinding_host_is_rejected(server,host):
    assert request(server,"/api/search",headers={"Host":host})[0] == 403
    assert request(server,"/",headers={"Host":host})[0] == 403


def test_duplicate_host_is_rejected(server):
    con=http.client.HTTPConnection("127.0.0.1",server.server_port)
    con.putrequest("GET","/")
    con.putheader("Host","attacker.invalid")
    con.endheaders()
    response=con.getresponse()
    assert response.status==403
    response.read()
    con.close()


@pytest.mark.parametrize("path,body",[("/api/estimate",{"sql":"SELECT 1"}),("/api/column-costs",{"fqn":"p.d.orders"}),("/api/preview",{"fqn":"p.d.orders"}),("/api/refresh-sample",{"fqn":"p.d.orders"}),("/api/cost-report",{"hours":24}), ("/api/refresh",{"projects":["p"]}), ("/api/table-queries",{"table":"p.d.orders"}), ("/api/inspect-job",{"job_id":"j","location":"US"})])
def test_simple_cross_origin_posts_never_invoke_bigquery(server,monkeypatch,path,body):
    action=Mock(side_effect=AssertionError("must reject before dispatch"))
    for name in ["dry_run","column_costs","preview","refresh_sample","cost_report", "table_queries", "inspect_query_job"]: monkeypatch.setattr(bq,name,action)
    monkeypatch.setattr(server.refresh, 'start', action)
    headers={"Origin":"https://attacker.invalid","Content-Type":"text/plain","Sec-Fetch-Site":"cross-site"}
    assert request(server,path,"POST",json.dumps(body),headers,authorized=False)[0]==403
    # Even a known token does not permit a hostile origin or a simple content type.
    assert request(server,path,"POST",json.dumps(body),headers)[0]==403
    assert request(server,path,"POST",json.dumps(body),{"Content-Type":"text/plain"})[0]==415
    action.assert_not_called()


def test_api_requires_token_and_preview_is_post_only(server):
    assert request(server,"/api/search",authorized=False)[0]==403
    assert request(server,"/api/search",headers={"X-Sounding-Token":"wrong"})[0]==403
    assert request(server,"/api/preview?fqn=p.d.orders")[0]==404
    assert request(server,"/api/search",headers={"Sec-Fetch-Site":"same-site"})[0]==403
    assert request(server,"/api/estimate","OPTIONS")[0]==405


def test_valid_actions_and_missing_table_errors(server,monkeypatch):
    monkeypatch.setattr(bq,"dry_run",lambda sql:{"bytes":123,"usd_on_demand":0.1})
    monkeypatch.setattr(bq,"preview",lambda fqn,n:{"rows":[{"id":1}],"receipt":{}})
    monkeypatch.setattr(bq,"cost_report",lambda hours:{"hours":hours,"job_count":0})
    assert json.loads(request(server,"/api/estimate","POST",json.dumps({"sql":"SELECT 1"}))[2])["bytes"]==123
    assert json.loads(request(server,"/api/preview","POST",json.dumps({"fqn":"p.d.orders"}))[2])["rows"]==[{"id":1}]
    assert json.loads(request(server,"/api/cost-report","POST",json.dumps({"hours":168}))[2])=={"hours":168,"job_count":0}
    assert request(server,"/api/table?fqn=p.d.missing")[0]==404
    status,_,body=request(server,"/api/column-costs","POST",json.dumps({"fqn":"p.d.missing"}))
    assert status==404 and "error" in json.loads(body)


@pytest.mark.parametrize("body",["{", "[]", "null", "1", '{"sql":null}'])
def test_malformed_payloads_have_json_errors(server,body):
    status,_,body=request(server,"/api/estimate","POST",body)
    assert status==400 and "error" in json.loads(body)


def test_body_limits_and_safe_errors(server,monkeypatch):
    assert request(server,"/api/estimate","POST","",{"Content-Length":"-1"})[0]==413
    assert request(server,"/api/estimate","POST","",{"Content-Length":str(ui.MAX_BODY+1)})[0]==413
    monkeypatch.setattr(bq,"dry_run",Mock(side_effect=RuntimeError("/private/secret credentials@example.com token-value")))
    status,_,body=request(server,"/api/estimate","POST",json.dumps({"sql":"SELECT 1"}))
    assert status==500
    assert b"/private" not in body and b"credentials" not in body and b"token-value" not in body


def test_simultaneous_requests_and_snapshot_publication(server):
    with ThreadPoolExecutor(8) as pool:
        futures=[pool.submit(request,server,"/api/search") for _ in range(8)]
        catalog.save_snapshot("p.other",[table_row("p.other.t")],[])
        assert all(f.result()[0]==200 for f in futures)


def test_busy_port_falls_forward():
    with ui.bind(0) as first:
        with ui.bind(first.server_port) as second:
            assert second.server_port != first.server_port


def test_port_boundaries_do_not_overflow(monkeypatch):
    calls=[]
    def busy(address):
        calls.append(address[1])
        raise OSError(errno.EADDRINUSE,"busy")
    monkeypatch.setattr(ui,"Server",busy)
    with pytest.raises(ValueError): ui.bind(65535)
    assert calls==[65535]
    for port in [-1,65536]:
        with pytest.raises(ValueError): ui.bind(port)


def test_browse_and_dataset_are_local_and_scoped(server, seed):
    seed([table_row('second.d.orders')], [], dataset='second.d')
    seed([], [], dataset='second.empty')
    tree = json.loads(request(server, '/api/browse')[2])
    assert [p['project'] for p in tree['projects']] == ['p', 'second']
    assert tree['projects'][1]['datasets'][1]['tables'] == 0
    page = json.loads(request(server, '/api/search?project=second&dataset=second.d')[2])
    assert [r['table'] for r in page['items']] == ['second.d.orders']
    dataset = json.loads(request(server, '/api/dataset?dataset=second.empty')[2])
    assert dataset['tables'] == 0 and dataset['routines'] == []
    assert request(server, '/api/dataset?dataset=second.missing')[0] == 404
    assert request(server, '/api/browse', authorized=False)[0] == 403
    assert request(server, '/api/dataset?dataset=p.d', authorized=False)[0] == 403


def test_refresh_and_history_endpoints_and_local_docs(server, monkeypatch):
    started = Mock(return_value={'id':'j','status':'running'})
    monkeypatch.setattr(server.refresh, 'start', started)
    assert request(server, '/api/refresh', 'POST', json.dumps({'datasets':['p.d'],'projects':['q']}))[0] == 202
    started.assert_called_once_with(['p.d'], ['q'])
    assert json.loads(request(server, '/api/refresh')[2]) == {'status':'idle'}
    assert request(server, '/api/refresh', authorized=False)[0] == 403
    query = Mock(return_value={'jobs':[]})
    monkeypatch.setattr(bq, 'table_queries', query)
    assert request(server, '/api/table-queries', 'POST', json.dumps({'table':'p.d.orders'}))[0] == 200
    query.assert_called_once_with('p.d.orders', 30, project=None, scope='user', rank_by='billed_bytes', limit=20)
    inspect = Mock(return_value={'sql':None})
    monkeypatch.setattr(bq, 'inspect_query_job', inspect)
    assert request(server, '/api/inspect-job', 'POST', json.dumps({'job_id':'j','location':'US'}))[0] == 200
    assert inspect.call_args.kwargs['include_sql'] is False
    docs = json.loads(request(server, '/api/docs')[2])['html']
    assert 'MCP' in docs and 'CLI' in docs and '/api/refresh' in docs


def test_inventory_refresh_and_hydration_routes(server, monkeypatch):
    from sounding import refresh
    seen=[]
    monkeypatch.setattr(refresh.Manager,'start',lambda self,datasets,projects,**opts: seen.append(opts) or {'status':'running'})
    options={'projects':['p'],'inventory_only':True,'exclude_datasets':['stage*'],'include_hidden':True,'project_concurrency':2,'resume':True}
    status,_,body=request(server,'/api/refresh','POST',json.dumps(options))
    assert status == 202 and seen[0]['inventory_only'] and seen[0]['resume']
    monkeypatch.setattr(bq,'load_table_metadata',lambda fqn: {'fqn':fqn,'metadata_level':'full'})
    status,_,body=request(server,'/api/load-metadata','POST',json.dumps({'fqn':'p.d.t'}))
    assert status == 200 and json.loads(body)['metadata_level'] == 'full'
