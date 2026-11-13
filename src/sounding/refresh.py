"""Metadata refresh shared by the UI, CLI and MCP; no authentication UI or SQL.

Each dataset publishes independently through the existing harvester. A separate
process lock prevents overlapping refresh jobs without blocking catalog reads.
"""

from __future__ import annotations

import copy
import datetime as dt
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from filelock import FileLock, Timeout

from . import bq, catalog, workload
from .errors import SoundingError, public_error, text
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
    def __init__(self, datasets=None, projects=None, progress=None, *, include_preview_rows=False):
        self.datasets, self.projects = targets(datasets, projects)
        if not isinstance(include_preview_rows, bool):
            raise SoundingError("include_preview_rows must be a boolean.")
        self.include_preview_rows = include_preview_rows
        self.progress = progress or Quiet()
        self.lock = threading.RLock()
        self.state = {"id": uuid.uuid4().hex, "status": "pending", "phase": "discovering",
                      "started_at": dt.datetime.now(dt.UTC).isoformat(), "finished_at": None,
                      "requested_datasets": self.datasets, "requested_projects": self.projects,
                      "include_preview_rows": include_preview_rows,
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
                with self.lock:
                    self.state["status"] = "running"
                self._run()
        except Exception as error:
            message = "Another metadata refresh is running for this catalog. Try again when it finishes." if isinstance(error, Timeout) else public_error(error)[0]
            with self.lock:
                self.state.update(status="failed", message=message)
        finally:
            with self.lock:
                self.state["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
            self.progress.close()
        return self.snapshot()

    def _run(self):
        with catalog.connect():
            pass
        datasets = list(self.datasets)
        for project in self.projects:
            row = {"project": project, "status": "running", "datasets": 0, "message": None}
            with self.lock:
                self.state["projects"].append(row)
            try:
                discovered = bq.project_datasets(project)
                with catalog.connect(write=True) as con:
                    con.execute("INSERT OR REPLACE INTO projects VALUES (?, ?)", [project, dt.datetime.now(dt.UTC)])
                datasets.extend(discovered["datasets"])
                with self.lock:
                    row.update(status="complete", datasets=len(discovered["datasets"]), receipt=discovered["receipt"])
                    if discovered["unreachable"]:
                        row.update(status="partial", message="Some locations were unreachable: " + ", ".join(discovered["unreachable"]))
            except Exception as error:
                with self.lock:
                    row.update(status="failed", message=public_error(error)[0])
        with self.lock:
            self.state["datasets"] = [{"dataset": ds, "status": "queued", "done": 0, "total": 0,
                                       "message": None} for ds in sorted(set(datasets))]
            self.state["phase"] = "harvesting"
        with ThreadPoolExecutor(bq.DATASET_WORKERS) as pool:
            list(pool.map(self._dataset, self.state["datasets"]))
        with self.lock:
            all_rows = self.state["projects"] + self.state["datasets"]
            failed = any(r["status"] in ("failed", "partial") for r in all_rows)
            succeeded = any(r["status"] in ("complete", "partial") for r in self.state["datasets"])
            if not self.state["datasets"]:
                succeeded = any(r["status"] == "complete" for r in self.state["projects"])
            self.state["status"] = "partial" if failed and succeeded else "failed" if failed else "complete"
            self.state["warnings"] = ["Project refresh discovers accessible, non-hidden datasets. Previously saved datasets that are no longer visible are retained; they are not evidence of current access."] if self.projects else []
            self.state["phase"] = "finished"

    def _dataset(self, row):
        with self.lock:
            row["status"] = "running"
        try:
            kwargs = {"include_preview_rows": True} if self.include_preview_rows else {}
            receipt = bq.harvest([row["dataset"]], DatasetProgress(self, row), **kwargs)
            coverage = receipt.get("coverage", {}).get(row["dataset"], {})
            with self.lock:
                row.update(status=coverage.get("status", "complete"), receipt=receipt,
                           message=" ".join(coverage.get("warnings", [])) or None)
        except Exception as error:
            with self.lock:
                row.update(status="failed", message=public_error(error)[0] + " Previous snapshot retained.")


class Manager:
    """One background refresh per server; polling survives a browser reload."""
    def __init__(self):
        self.lock = threading.Lock()
        self.job = None

    def start(self, datasets=None, projects=None):
        job = Job(datasets, projects)
        with self.lock:
            if self.job and self.job.snapshot()["status"] in ("pending", "running"):
                raise SoundingError("A metadata refresh is already running.")
            self.job = job
            threading.Thread(target=job.run, daemon=True).start()
            return job.snapshot()

    def snapshot(self):
        with self.lock:
            return self.job.snapshot() if self.job else {"status": "idle"}
