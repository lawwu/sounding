import sys
import json

import pytest
from google.auth.exceptions import DefaultCredentialsError, RefreshError

from sounding import bq, cli
from sounding.errors import public_error
from conftest import table_row


def run(monkeypatch, capsys, *args):
    monkeypatch.setattr(sys, "argv", ["sounding", *args])
    try:
        cli.main()
    except SystemExit as exit:
        return exit.code, capsys.readouterr()
    return 0, capsys.readouterr()


@pytest.mark.parametrize("error,text,status", [
    (DefaultCredentialsError("none"), "No Google credentials found", 401),
    (RefreshError("expired"), "expired or were revoked", 401)])
def test_credential_errors_say_how_to_log_in(error, text, status):
    message, code = public_error(error)
    assert text in message and "gcloud auth application-default login" in message and code == status


def test_cli_credential_error(monkeypatch, capsys):
    def no_credentials(*a, **k):
        raise DefaultCredentialsError("Your default credentials were not found.")
    monkeypatch.setattr(bq, "harvest", no_credentials)
    code, _ = run(monkeypatch, capsys, "harvest", "p.d")
    assert "No Google credentials found" in code


def test_cli_names_unexpected_errors(monkeypatch, capsys):
    def broken(*a, **k):
        raise KeyError("tableReference")
    monkeypatch.setattr(bq, "harvest", broken)
    code, _ = run(monkeypatch, capsys, "harvest", "p.d")
    assert code == "sounding: The operation failed: KeyError: 'tableReference'"


def test_show_prints_unknown_sizes_as_unknown(seed, monkeypatch, capsys):
    seed([table_row(), table_row("p.d.v", type="VIEW", num_rows=None, num_bytes=None)], [])
    code, out = run(monkeypatch, capsys, "show")
    view = next(line for line in out.out.splitlines() if line.endswith("p.d.v"))
    assert code == 0 and view.split()[:2] == ["unknown", "unknown"]
    assert "0.00" not in view


@pytest.mark.parametrize("execute", [False, True])
def test_query_opportunities_cli_explicit_billing_and_options(monkeypatch, capsys, execute):
    seen = {}
    def scan(**kwargs):
        seen.update(kwargs)
        return {"status": "complete" if kwargs["allow_billed"] else "preview"}
    monkeypatch.setattr(bq, "query_opportunities", scan)
    args = ["query-opportunities", "--location", "EU", "--project", "p",
            "--scope", "project", "--rank-by", "slot_ms", "--hours", "24"]
    if execute:
        args.append("--allow-billed")
    code, out = run(monkeypatch, capsys, *args)
    assert code == 0 and json.loads(out.out)["status"] == ("complete" if execute else "preview")
    assert seen["allow_billed"] is execute and seen["location"] == "EU"
    assert seen["project"] == "p" and seen["scope"] == "project"
    assert seen["rank_by"] == "slot_ms" and seen["hours"] == 24


@pytest.mark.parametrize("status", ["blocked", "failed"])
def test_unsuccessful_scan_preserves_json_and_exits_nonzero(monkeypatch, capsys, status):
    monkeypatch.setattr(bq, "query_opportunities", lambda **k: {"status": status, "receipt": {"job_id": "j"}})
    code, out = run(monkeypatch, capsys, "query-opportunities", "--location", "US", "--allow-billed")
    assert code == 2 and json.loads(out.out)["receipt"]["job_id"] == "j"


def test_query_report_cli_is_local(monkeypatch, capsys):
    from sounding import catalog
    saved = catalog.save_workload_report({"status": "complete", "project": "p"})
    code, out = run(monkeypatch, capsys, "query-report", saved["report_id"])
    assert code == 0 and json.loads(out.out) == saved


def test_refresh_cli_partial_results_and_table_history(monkeypatch, capsys):
    from google.api_core.exceptions import Forbidden
    monkeypatch.setattr(bq, 'project_datasets', lambda p: {'datasets':[p+'.d'], 'unreachable':[], 'receipt':{}})
    monkeypatch.setattr(bq, 'harvest', lambda *args: (_ for _ in ()).throw(Forbidden('denied')))
    code, output = run(monkeypatch, capsys, 'refresh', '--project', 'p')
    assert code == 2
    assert json.loads(output.out)['datasets'][0]['status'] == 'failed'
    seen = []
    monkeypatch.setattr(bq, 'table_queries', lambda table,days,**kwargs: seen.append((table,days,kwargs)) or {'jobs':[]})
    code, output = run(monkeypatch, capsys, 'table-queries', 'p.d.t', '--scope', 'project')
    assert code == 0 and seen[0][1] == 30 and seen[0][2]['scope'] == 'project'


@pytest.mark.parametrize("command", ["harvest", "refresh"])
@pytest.mark.parametrize("include", [False, True])
def test_preview_rows_flag_is_explicit_and_warns(monkeypatch, capsys, command, include):
    seen = []
    monkeypatch.setattr(bq, "harvest", lambda datasets, progress, **kwargs: seen.append(kwargs) or bq.free_api_receipt("tables.get"))
    args = [command, "p.d"] + (["--include-preview-rows"] if include else [])
    code, output = run(monkeypatch, capsys, *args)
    assert code == 0
    assert seen == ([{"include_preview_rows": True}] if include else [{}])
    assert ("material delay" in output.err) is include
