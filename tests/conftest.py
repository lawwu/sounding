import datetime as dt

import pytest

from sounding import bq, catalog


@pytest.fixture(autouse=True)
def isolated_catalog(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "CATALOG", tmp_path / "catalog.duckdb")
    monkeypatch.setattr(bq, "client", lambda: pytest.fail("Unmocked BigQuery access in an offline test"))


def table_row(fqn="p.d.t", **overrides):
    project, dataset, name = fqn.split(".")
    return dict(dataset=f"{project}.{dataset}", table_id=name, fqn=fqn, type="TABLE",
                num_rows=10, num_bytes=2**40, created=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                modified=dt.datetime(2026, 1, 2, tzinfo=dt.UTC), partitioning=None,
                clustering=None, description=None, view_sql=None) | overrides


def column_row(path="id", fqn="p.d.t", **overrides):
    return dict(fqn=fqn, path=path, type="STRING", mode="NULLABLE", description=None) | overrides


@pytest.fixture
def seed():
    def save(tables=None, columns=None, dataset="p.d"):
        return catalog.save_snapshot(dataset, [table_row()] if tables is None else tables,
                                     [column_row()] if columns is None else columns)
    return save
