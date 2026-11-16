"""Metadata refresh shared by the UI, CLI and MCP; no authentication UI or SQL.

Each dataset publishes independently through the existing harvester. A separate
process lock prevents overlapping refresh jobs without blocking catalog reads.
"""

from __future__ import annotations

import copy
import json
import logging
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


ACTIVE_STATUSES = frozenset({"pending", "running"})
FINISHED_STATUSES = frozenset({"complete", "partial", "failed", "interrupted"})
FINISHED_ROWS = frozenset({"complete", "partial", "failed"})
# Resume restores work into a new pending attempt; it never reopens a finished Job.
TRANSITIONS = {
    "pending": {"running", "failed"},
    "running": FINISHED_STATUSES,
    **{status: set() for status in FINISHED_STATUSES},
}


def _snapshot(state):
    result = copy.deepcopy(state)
    rows = result["datasets"]
    result["progress"] = {"datasets_done": sum(r["status"] in FINISHED_ROWS for r in rows),
                          "datasets_total": len(rows), "done": sum(r["done"] for r in rows),
                          "total": sum(r["total"] for r in rows)}
    return result


def _read_checkpoint(path):
    """Check persisted state before polling or resuming it; leave bad files untouched."""
    try:
        state = json.loads(path.read_text())
        if not isinstance(state, dict) or state.get("status") not in ACTIVE_STATUSES | FINISHED_STATUSES:
            raise ValueError
        if not isinstance(state.get("id"), str) or not isinstance(state.get("started_at"), str):
            raise ValueError
        dt.datetime.fromisoformat(state["started_at"])
        if state.get("finished_at") is not None:
            dt.datetime.fromisoformat(state["finished_at"])
        for collection, identity in (("projects", "project"), ("datasets", "dataset")):
            if not isinstance(state.get(collection), list):
                raise ValueError
            for row in state[collection]:
                if (not isinstance(row, dict) or not isinstance(row.get(identity), str)
                        or row.get("status") not in {"queued", "running"} | FINISHED_ROWS | {"interrupted"}):
                    raise ValueError
                if collection == "datasets" and any(type(row.get(k)) is not int or row[k] < 0 for k in ("done", "total")):
                    raise ValueError
        return _snapshot(state)
    except FileNotFoundError:
        raise
    except (OSError, ValueError, TypeError) as error:
        raise SoundingError("Saved refresh progress could not be read. Start a new refresh instead of resuming.") from error


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
        self._started = False
        self.finished = threading.Event()
        self.checkpoint_saved = False
        self.state = {"id": uuid.uuid4().hex, "status": "pending", "phase": "discovering",
                      "started_at": dt.datetime.now(dt.UTC).isoformat(), "finished_at": None,
                      "requested_datasets": self.datasets, "requested_projects": self.projects,
                      "include_preview_rows": include_preview_rows, "inventory_only": inventory_only,
                      "filters": self.filters, "project_concurrency": project_concurrency, "excluded_datasets": [],
                      "projects": [], "datasets": [], "warnings": [], "message": None}

    def snapshot(self):
        with self.lock:
            return _snapshot(self.state)

    def _transition(self, status, *, message=None):
        """Change job status, phase and timestamps together, under the snapshot lock."""
        with self.lock:
            if status not in TRANSITIONS[self.state["status"]]:
                raise RuntimeError(f"Invalid refresh transition: {self.state['status']} -> {status}")
            self.state.update(status=status, message=message,
                              phase="discovering" if status == "running" else "finished",
                              finished_at=None if status == "running" else dt.datetime.now(dt.UTC).isoformat())
            if status == "running":
                self.state["started_at"] = dt.datetime.now(dt.UTC).isoformat()
            else:
                for row in self.state["projects"] + self.state["datasets"]:
                    if row["status"] == "running":
                        row.update(status="interrupted", message="This attempt stopped before the work finished.")

    def run(self):
        with self.lock:
            if self._started:
                raise SoundingError("This refresh job has already started. Create a new job to refresh or resume.")
            self._started = True
        try:
            catalog.CATALOG.parent.mkdir(parents=True, exist_ok=True)
            with FileLock(str(catalog.CATALOG) + ".refresh.lock", timeout=0):
                if self.resume:
                    self._restore()
                self._transition("running")
                try:
                    self._checkpoint()
                    self._transition(self._run())
                except Exception as error:
                    self._transition("failed", message=public_error(error)[0])
                except BaseException:
                    self._transition("interrupted", message="Import interrupted. Resume the saved import to continue.")
                    raise
                finally:
                    # Persist terminal failures as well as successes, before releasing
                    # ownership. Failure to save progress cannot undo published data.
                    try:
                        self._checkpoint()
                    except Exception:
                        logging.exception("Could not save refresh progress")
                        with self.lock:
                            self.state["warnings"].append("Could not save refresh progress. The saved checkpoint may be out of date; inspect the catalog before resuming.")
        except Exception as error:
            message = "Another metadata refresh is running for this catalog. Try again when it finishes." if isinstance(error, Timeout) else public_error(error)[0]
            self._transition("failed", message=message)
        finally:
            try:
                self.progress.close()
            finally:
                self.finished.set()
        return self.snapshot()

    def _checkpoint(self):
        with self.lock:
            temporary = self.checkpoint.with_suffix('.json.tmp')
            temporary.write_text(json.dumps(self.snapshot()))
            os.replace(temporary, self.checkpoint)
            self.checkpoint_saved = True

    def _restore(self):
        if not self.checkpoint.exists():
            raise SoundingError("No saved refresh exists for this catalog.")
        previous = _read_checkpoint(self.checkpoint)
        for key in ('requested_datasets', 'requested_projects', 'include_preview_rows', 'inventory_only', 'filters'):
            if previous.get(key) != self.state[key]:
                raise SoundingError("Resume requires the same projects, datasets, filters and import mode as the saved refresh.")
        previous.update(project_concurrency=self.project_concurrency, status="pending", phase="discovering",
                        finished_at=None, message=None, warnings=[])
        for row in previous["projects"] + previous["datasets"]:
            if row["status"] != "complete":
                row.update(status="queued", message=None)
                row.pop("receipt", None)
                if "dataset" in row:
                    row.update(done=0, total=0)
        with self.lock:
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
        with self.lock:
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
            outcome = "partial" if failed and succeeded else "failed" if failed else "complete"
            self.state["warnings"] = ["Dataset filters apply to this refresh only. Previously saved datasets are retained; they are not evidence of current access."]
            if self.inventory_only:
                self.state['warnings'].append('Inventory mode omits schemas, view SQL and auxiliary metadata. Load table metadata explicitly for details.')
            return outcome


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
            if self.job and not self.job.finished.is_set():
                raise SoundingError("A metadata refresh is already running.")
            previous = self.job
            self.job = job
            try:
                threading.Thread(target=job.run, daemon=True).start()
            except BaseException:
                self.job = previous
                raise
            return job.snapshot()

    def snapshot(self):
        with self.lock:
            local = self.job.snapshot() if self.job else None
            if local and not self.job.finished.is_set():
                return local
            def observed(state):
                # Keep submission errors separate from the import that can actually
                # be resumed. Rejected requests never published their own checkpoint.
                if local and not self.job.checkpoint_saved and local.get('message'):
                    state['submission_error'] = local['message']
                return state
            checkpoint = Path(str(catalog.CATALOG) + '.refresh.json')
            try:
                state = _read_checkpoint(checkpoint)
            except FileNotFoundError:
                return local or {"status": "idle"}
            except SoundingError as error:
                return {"status": "interrupted", "message": str(error)}
            if state['status'] in ACTIVE_STATUSES:
                try:
                    with FileLock(str(catalog.CATALOG) + '.refresh.lock', timeout=0):
                        # The worker may have finished between the read and the lock.
                        state = _read_checkpoint(checkpoint)
                        if state['status'] in ACTIVE_STATUSES:
                            state.update(status='interrupted', phase='finished', finished_at=None,
                                         message='Import interrupted. Resume the saved import to continue.')
                            for row in state['projects'] + state['datasets']:
                                if row['status'] == 'running':
                                    row.update(status='interrupted')
                except Timeout:
                    return observed(state)  # A CLI or another server still owns the refresh.
                except (FileNotFoundError, SoundingError):
                    return {"status": "interrupted", "message": "Saved refresh progress changed. Read status again or start a new refresh."}
            # Atomic publication, not wall-clock ordering, identifies the latest
            # attempt. Prefer memory only for our own checkpoint (e.g. final save
            # failed); a clock adjustment cannot hide another process's import.
            if (local and self.job.checkpoint_saved
                    and (local['id'], local['started_at']) == (state['id'], state['started_at'])):
                return local
            return observed(state)
