"""Metadata refresh shared by the UI, CLI and MCP; no authentication UI or SQL.

Each dataset publishes independently through the existing harvester. A separate
process lock prevents overlapping refresh jobs without blocking catalog reads.
"""

from __future__ import annotations

import copy
import json
import os
import time
from pathlib import Path
from fnmatch import fnmatchcase
from google.api_core.exceptions import TooManyRequests, ServiceUnavailable, InternalServerError, GatewayTimeout, DeadlineExceeded, RetryError
import datetime as dt
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from requests.exceptions import ConnectionError as NetworkConnectionError, Timeout as NetworkTimeout

from filelock import FileLock, Timeout

from . import bq, catalog, workload
from .errors import SoundingError, public_error, text, integer
from .progress import Quiet


def targets(datasets=None, projects=None):
    datasets = [] if datasets is None else datasets
    projects = [] if projects is None else projects
    if not isinstance(datasets, list) or not isinstance(projects, list):
        raise SoundingError("datasets and projects must be lists.")
    if len(datasets) > 1000 or len(projects) > 25 or not (datasets or projects):
        raise SoundingError("Select at least one dataset or project, up to 1000 datasets and 25 projects per refresh.")
    normalized = []
    for dataset in datasets:
        text(dataset, "dataset", 1300)
        project, separator, name = dataset.rpartition(".")
        if not separator or not re.fullmatch(r"[A-Za-z0-9_]{1,1024}", name):
            raise SoundingError("Use a fully qualified dataset: project.dataset.")
        normalized.append(workload.identifier(project, "project") + "." + name)
    return list(dict.fromkeys(normalized)), list(dict.fromkeys(workload.identifier(p, "project") for p in projects))


class DatasetProgress(Quiet):
    def __init__(self, job, row):
        super().__init__()
        self.job, self.row = job, row

    def add(self, n=1):
        with self.job.lock:
            self.row["total"] += n
        self.job.progress.add(n)

    def advance(self, n=1):
        with self.job.lock:
            self.row["done"] += n
        self.job.progress.advance(n)

    def log(self, line):
        self.job.progress.log(line)


class Job:
    def __init__(self, datasets=None, projects=None, progress=None, *, include_preview_rows=False, inventory_only=False, include_datasets=None,
                 exclude_datasets=None, include_hidden=False, project_concurrency=1, resume=False):
        self.datasets, self.projects = targets(datasets, projects)
        if not isinstance(include_preview_rows, bool):
            raise SoundingError("include_preview_rows must be a boolean.")
        for name, value in (("inventory_only", inventory_only), ("include_hidden", include_hidden), ("resume", resume)):
            if not isinstance(value, bool):
                raise SoundingError(f"{name} must be a boolean.")
        if inventory_only and include_preview_rows:
            raise SoundingError("Inventory mode cannot include preview rows.")
        integer(project_concurrency, "project_concurrency", 1, 4)
        self.filters = {"include_datasets": [] if include_datasets is None else include_datasets, "exclude_datasets": [] if exclude_datasets is None else exclude_datasets, "include_hidden": include_hidden}
        for key in ('include_datasets', 'exclude_datasets'):
            if not isinstance(self.filters[key], list) or len(self.filters[key]) > 100:
                raise SoundingError(f"{key} must be a list of up to 100 patterns.")
            for pattern in self.filters[key]:
                text(pattern, key, 1300)
        self.inventory_only, self.project_concurrency, self.resume = inventory_only, project_concurrency, resume
        self.checkpoint = Path(str(catalog.CATALOG) + '.refresh.json')
        self.include_preview_rows = include_preview_rows
        self.progress = progress or Quiet()
        self.lock = threading.RLock()
        self.state = {"id": uuid.uuid4().hex, "status": "pending", "phase": "discovering",
                      "started_at": dt.datetime.now(dt.UTC).isoformat(), "finished_at": None,
                      "requested_datasets": self.datasets, "requested_projects": self.projects,
                      "include_preview_rows": include_preview_rows, "inventory_only": inventory_only,
                      "filters": self.filters, "project_concurrency": project_concurrency, "excluded_datasets": [],
                      "projects": [], "datasets": [], "warnings": [], "message": None}

    def snapshot(self):
        with self.lock:
            result = copy.deepcopy(self.state)
        rows = result["datasets"]
        result["progress"] = {"datasets_done": sum(r["status"] in ("complete", "partial", "failed") for r in rows),
                              "datasets_total": len(rows), "done": sum(r["done"] for r in rows),
                              "total": sum(r["total"] for r in rows)}
        return result

    def run(self):
        try:
            catalog.CATALOG.parent.mkdir(parents=True, exist_ok=True)
            with FileLock(str(catalog.CATALOG) + ".refresh.lock", timeout=0):
                if self.resume:
                    self._restore()
                with self.lock:
                    self.state["status"] = "running"
                    self.state["finished_at"] = None
                    self.state["message"] = None
                self._checkpoint()
                self._run()
                self.state["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
                self._checkpoint()
        except Exception as error:
            message = "Another metadata refresh is running for this catalog. Try again when it finishes." if isinstance(error, Timeout) else public_error(error)[0]
            with self.lock:
                self.state.update(status="failed", message=message)
        finally:
            with self.lock:
                self.state["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
            self.progress.close()
        return self.snapshot()

    def _checkpoint(self):
        with self.lock:
            temporary = self.checkpoint.with_suffix('.json.tmp')
            temporary.write_text(json.dumps(self.snapshot()))
            os.replace(temporary, self.checkpoint)

    def _restore(self):
        if not self.checkpoint.exists():
            raise SoundingError("No saved refresh exists for this catalog.")
        previous = json.loads(self.checkpoint.read_text())
        for key in ('requested_datasets', 'requested_projects', 'include_preview_rows', 'inventory_only', 'filters'):
            if previous.get(key) != self.state[key]:
                raise SoundingError("Resume requires the same projects, datasets, filters and import mode as the saved refresh.")
        previous['project_concurrency'] = self.project_concurrency
        self.state = previous

    def _allowed(self, dataset):
        name = dataset.rsplit('.', 1)[-1]
        def matches(patterns):
            return any(fnmatchcase(dataset, p) or fnmatchcase(name, p) for p in patterns)
        return ((self.filters['include_hidden'] or not name.startswith('_'))
                and (not self.filters['include_datasets'] or matches(self.filters['include_datasets']))
                and not matches(self.filters['exclude_datasets']))

    def _retry(self, call, row):
        for attempt in range(3):
            try:
                return call()
            except (TooManyRequests, ServiceUnavailable, InternalServerError, GatewayTimeout, DeadlineExceeded, RetryError, NetworkConnectionError, NetworkTimeout):
                if attempt == 2:
                    raise
                with self.lock:
                    row['retries'] = row.get('retries', 0) + 1
                time.sleep(2 ** attempt)

    def _run(self):
        with catalog.connect():
            pass
        datasets = list(self.datasets)
        previous_projects = {r['project']: r for r in self.state['projects']}
        self.state['projects'] = [previous_projects.get(p, {"project": p, "status": "queued", "datasets": 0, "message": None}) for p in self.projects]
        def discover(row):
            project = row['project']
            if row['status'] == 'complete' and 'discovered_datasets' in row:
                return
            with self.lock:
                row.update(status='running', message=None)
            try:
                kwargs = {'include_hidden': True} if self.filters['include_hidden'] else {}
                discovered = self._retry(lambda: bq.project_datasets(project, **kwargs), row)
                with catalog.connect(write=True) as con:
                    con.execute("INSERT OR REPLACE INTO projects VALUES (?, ?)", [project, dt.datetime.now(dt.UTC)])
                with self.lock:
                    row.update(status="complete", datasets=len(discovered["datasets"]), receipt=discovered["receipt"],
                               discovered_datasets=discovered['datasets'])
                    if discovered["unreachable"]:
                        row.update(status="partial", message="Some locations were unreachable: " + ", ".join(discovered["unreachable"]))
            except Exception as error:
                with self.lock:
                    row.update(status="failed", message=public_error(error)[0])
            self._checkpoint()
        with ThreadPoolExecutor(self.project_concurrency) as pool:
            list(pool.map(discover, self.state['projects']))
        for row in self.state['projects']:
            datasets.extend(row.get('discovered_datasets', []))
        previous_datasets = {r['dataset']: r for r in self.state['datasets']}
        with self.lock:
            self.state['excluded_datasets'] = sorted(ds for ds in set(datasets) if not self._allowed(ds))
            self.state["datasets"] = [previous_datasets.get(ds, {"dataset": ds, "status": "queued", "done": 0, "total": 0,
                                       "message": None}) for ds in sorted(set(datasets)) if self._allowed(ds)]
            self.state["phase"] = "harvesting"
        self._checkpoint()
        groups = {}
        for row in self.state['datasets']:
            groups.setdefault(row['dataset'].rsplit('.', 1)[0], []).append(row)
        def harvest_project(rows):
            with ThreadPoolExecutor(bq.DATASET_WORKERS) as pool:
                list(pool.map(self._dataset, rows))
        with ThreadPoolExecutor(self.project_concurrency) as pool:
            list(pool.map(harvest_project, groups.values()))
        with self.lock:
            all_rows = self.state["projects"] + self.state["datasets"]
            failed = any(r["status"] in ("failed", "partial") for r in all_rows)
            succeeded = any(r["status"] in ("complete", "partial") for r in self.state["datasets"])
            if not self.state["datasets"]:
                succeeded = any(r["status"] == "complete" for r in self.state["projects"])
            self.state["status"] = "partial" if failed and succeeded else "failed" if failed else "complete"
            self.state["warnings"] = ["Dataset filters apply to this refresh only. Previously saved datasets are retained; they are not evidence of current access."]
            if self.inventory_only:
                self.state['warnings'].append('Inventory mode omits schemas, view SQL and auxiliary metadata. Load table metadata explicitly for details.')
            self.state["phase"] = "finished"


    def _dataset(self, row):
        if row['status'] == 'complete':
            return
        with self.lock:
            row.update(status="running", done=0, total=0, message=None)
        try:
            kwargs = {"include_preview_rows": True} if self.include_preview_rows else {}
            if self.inventory_only:
                kwargs['inventory_only'] = True
            receipt = self._retry(lambda: bq.harvest([row["dataset"]], DatasetProgress(self, row), **kwargs), row)
            receipt['filters'] = self.filters
            coverage = receipt.get("coverage", {}).get(row["dataset"], {})
            with self.lock:
                row.update(status=coverage.get("status", "complete"), receipt=receipt,
                           message=" ".join(coverage.get("warnings", [])) or None)
        except Exception as error:
            with self.lock:
                row.update(status="failed", message=public_error(error)[0] + " Previous snapshot retained.")
        self._checkpoint()


class Manager:
    """One background refresh per server; polling survives a browser reload."""
    def __init__(self):
        self.lock = threading.Lock()
        self.job = None

    def start(self, datasets=None, projects=None, **options):
        job = Job(datasets, projects, **options)
        with self.lock:
            if self.job and self.job.snapshot()["status"] in ("pending", "running"):
                raise SoundingError("A metadata refresh is already running.")
            self.job = job
            threading.Thread(target=job.run, daemon=True).start()
            return job.snapshot()

    def snapshot(self):
        with self.lock:
            if self.job:
                return self.job.snapshot()
            checkpoint = Path(str(catalog.CATALOG) + '.refresh.json')
            try:
                state = json.loads(checkpoint.read_text())
            except (OSError, ValueError):
                return {"status": "idle"}
            if state['status'] in ('pending', 'running'):
                try:
                    with FileLock(str(catalog.CATALOG) + '.refresh.lock', timeout=0):
                        state.update(status='interrupted', message='Import interrupted. Resume the saved import to continue.')
                except Timeout:
                    pass  # A CLI or another server still owns the refresh.
            return state
