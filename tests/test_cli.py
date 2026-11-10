import sys

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
