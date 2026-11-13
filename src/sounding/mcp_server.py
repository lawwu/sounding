"""stdio MCP tools with bounded inputs and protocol-level, sanitized errors."""

from __future__ import annotations

import logging
from functools import wraps
from typing import Annotated

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from . import bq, catalog, workload, refresh
from .errors import public_error


def tool_errors(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as error:
            message, status = public_error(error)
            if status >= 500:
                logging.exception("Sounding tool failed")
            raise ToolError(message) from error
    return wrapped


def create_server() -> MCPServer:
    app = MCPServer("sounding")
    refresh_manager = refresh.Manager()

    @app.tool(description="Saved project → dataset hierarchy with table counts and harvest timestamps. Local only; no Google calls.")
    @tool_errors
    def browse_catalog() -> dict:
        with catalog.connect() as con:
            return catalog.browse(con)

    @app.tool(description="Start a background metadata refresh for explicit project.dataset names and/or whole projects. "
              "Projects discover all accessible non-hidden datasets, including newly added datasets. Free metadata APIs only, "
              "no SQL; uses existing Google credentials. Poll refresh_status on this MCP server for progress and per-dataset "
              "results. Failed datasets retain their previous snapshots. Only one refresh per catalog can run at a time.")
    @tool_errors
    def refresh_metadata(datasets: Annotated[list[str] | None, Field(max_length=1000)] = None,
                         projects: Annotated[list[str] | None, Field(max_length=25)] = None) -> dict:
        return refresh_manager.start(datasets, projects)

    @app.tool(description="Poll the current or last metadata refresh started by this MCP server. Local only. "
              "Returns status, progress, per-project discovery results and per-dataset outcomes. A restart clears this "
              "in-memory status, but published catalog snapshots are retained.")
    @tool_errors
    def refresh_status() -> dict:
        return refresh_manager.snapshot()

    @app.tool(description="Literal substring search of harvested tables, columns and descriptions. "
              "Local only; never calls BigQuery. Returns items and next_offset; pass next_offset as "
              "offset for another page. Matching column paths are sampled (20 per table); "
              "matched_column_count gives the full count. Descriptions are excerpts.")
    @tool_errors
    def search_catalog(q: Annotated[str, Field(max_length=2048)] = "",
                       limit: Annotated[int, Field(ge=1, le=100)] = 50,
                       offset: Annotated[int, Field(ge=0, le=1_000_000)] = 0,
                       project: Annotated[str, Field(max_length=2048)] = "",
                       dataset: Annotated[str, Field(max_length=2048)] = "") -> dict:
        with catalog.connect() as con:
            return catalog.search(con, q, limit, offset, project=project, dataset=dataset)

    @app.tool(description="Schema, size, UTC freshness, partitioning and clustering for "
              "project.dataset.table in the latest snapshot, plus declared (unenforced) keys, "
              "partition-filter requirements, storage bytes, labels, column policy tags, expiry, "
              "snapshot/clone source and external or materialized-view settings. Read `notes` "
              "before writing a query. Local only; never calls BigQuery. Missing values are null "
              "(unknown), never zero.")
    @tool_errors
    def describe_table(table: Annotated[str, Field(min_length=1, max_length=2048)]) -> dict:
        with catalog.connect() as con:
            return catalog.describe(con, table)

    @app.tool(description="Dataset location, description, labels, default expirations and storage "
              "billing model, with its tables count and routine signatures (no bodies). Local only; "
              "never calls BigQuery. routines_status is 'denied' when routines could not be listed.")
    @tool_errors
    def describe_dataset(dataset: Annotated[str, Field(min_length=1, max_length=2048)]) -> dict:
        with catalog.connect() as con:
            return catalog.describe_dataset(con, dataset)

    @app.tool(description="Saved BigQuery model metadata: type, features, labels, location and remote model settings. Local only; no training or inference.")
    @tool_errors
    def describe_model(model: Annotated[str, Field(min_length=1, max_length=2048)]) -> dict:
        with catalog.connect() as con:
            return catalog.describe_model(con, model)

    @app.tool(description="Read cached partition/cluster recommendations and table insights for a project. Includes coverage and timestamps; may be unavailable or stale. Local only; never applies suggestions.")
    @tool_errors
    def project_recommendations(project: Annotated[str, Field(min_length=1, max_length=2048)]) -> dict:
        with catalog.connect() as con:
            return catalog.recommendations(con, project)

    @app.tool(description="Literal substring search of harvested functions and procedures by name, "
              "description or body. Local only; never calls BigQuery. Bodies are 1000-character "
              "excerpts; use describe_routine for the whole definition.")
    @tool_errors
    def search_routines(q: Annotated[str, Field(max_length=2048)] = "",
                        limit: Annotated[int, Field(ge=1, le=100)] = 50,
                        offset: Annotated[int, Field(ge=0, le=1_000_000)] = 0) -> dict:
        with catalog.connect() as con:
            return catalog.search_routines(con, q, limit, offset)

    @app.tool(description="One function or procedure (project.dataset.routine): type, language, "
              "signature, return type, description and full definition. Local only.")
    @tool_errors
    def describe_routine(routine: Annotated[str, Field(min_length=1, max_length=2048)]) -> dict:
        with catalog.connect() as con:
            return catalog.describe_routine(con, routine)

    @app.tool(description="1–50 sample rows via free tables.get and tabledata.list calls. "
              "Native tables and snapshots only; views/external tables are rejected, never queried. "
              "BYTES are base64; decimal and temporal values are strings. Returns rows and a receipt.")
    @tool_errors
    def preview_rows(table: Annotated[str, Field(min_length=1, max_length=2048)],
                     n: Annotated[int, Field(ge=1, le=50)] = 5) -> dict:
        return bq.preview(table, n)

    @app.tool(description="Free dry run; SQL never executes. Returns estimated bytes and a US "
              "on-demand scan-price range including billing minimums. Unknown/incomplete estimates "
              "are null, never a promise of $0. Read estimate_status, warnings and price_basis. "
              "Scripts, dynamic SQL, external data and row security may be unestimated. "
              "This is not a spending cap. `receipt` copies BigQuery's own evidence that the dry "
              "run was free: dry_run_confirmed, billed_bytes (0) and job_id (null).")
    @tool_errors
    def estimate_query_cost(sql: Annotated[str, Field(min_length=1, max_length=bq.MAX_SQL_LENGTH)]) -> dict:
        return bq.dry_run(sql)

    @app.tool(description="Jobs this identity ran in the job project over the last `hours` (default 24, "
              "up to 4320), with BigQuery's billed bytes and an on-demand list-price total. Uses the free "
              "jobs.list call. Opt-in query-opportunities scans appear here; metadata and dry runs "
              "do not. Use it to show what a session cost, with evidence; billing reports are final.")
    @tool_errors
    def cost_report(hours: Annotated[int, Field(ge=1, le=bq.COST_REPORT_MAX_HOURS)] = 24) -> dict:
        return bq.cost_report(hours)

    @app.tool(description="Top individual completed query jobs referencing a table, default last 30 days, using free jobs.list. "
              "Searches the configured job project unless project is supplied. User scope is the current identity; project "
              "scope needs permission to see other users' job metadata. Scans at most the newest 1000 top-level jobs. "
              "Inspect warnings/truncated/missing_references; script children and other projects are excluded. "
              "Metrics are whole-job totals, not cost allocated to the table. No SQL text is returned or stored.")
    @tool_errors
    def table_query_history(table: Annotated[str, Field(min_length=1, max_length=2048)],
                            days: Annotated[int, Field(ge=1, le=180)] = 30,
                            project: Annotated[str | None, Field(max_length=256)] = None,
                            scope: Annotated[str, Field(pattern="^(user|project)$")] = "user",
                            rank_by: Annotated[str, Field(pattern="^(billed_bytes|slot_ms)$")] = "billed_bytes",
                            limit: Annotated[int, Field(ge=1, le=100)] = 20) -> dict:
        return bq.table_queries(table, days, project=project, scope=scope, rank_by=rank_by, limit=limit)

    @app.tool(description="Find recurring costly queries using fixed regional INFORMATION_SCHEMA SQL. "
              "Default is a FREE dry-run preview. Set allow_billed=true only when the user has authorized "
              "a billed metadata scan; maximum_bytes_billed is enforced, but does not cap slot charges. "
              "Use rank_by=slot_ms for capacity workloads. Defaults to this identity's jobs; project scope "
              "requires jobs.listAll. Groups by normalized hash and excludes cached jobs, failures and "
              "script parents. Findings are investigation leads, not proven savings. Completed reports "
              "are saved locally; read query_opportunity_report to reuse them without another scan. "
              "No business SQL is executed or stored. Inspect status, warnings and receipt.")
    @tool_errors
    def query_opportunities(
        location: Annotated[str, Field(min_length=1, max_length=256)],
        project: Annotated[str | None, Field(max_length=256)] = None,
        hours: Annotated[int, Field(ge=1, le=bq.COST_REPORT_MAX_HOURS)] = 168,
        scope: Annotated[str, Field(pattern="^(user|project)$")] = "user",
        rank_by: Annotated[str, Field(pattern="^(billed_bytes|slot_ms)$")] = "billed_bytes",
        limit: Annotated[int, Field(ge=1, le=100)] = 20,
        max_bytes_billed: Annotated[int, Field(ge=workload.MIN_BYTES, le=workload.MAX_BYTES)] = workload.DEFAULT_MAX_BYTES,
        min_billed_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)] = workload.DEFAULT_MIN_BILLED_BYTES,
        min_slot_ms: Annotated[int, Field(ge=0, le=2**63 - 1)] = workload.DEFAULT_MIN_SLOT_MS,
        allow_billed: bool = False,
    ) -> dict:
        return bq.query_opportunities(location, project=project, hours=hours, scope=scope,
            rank_by=rank_by, limit=limit, max_bytes_billed=max_bytes_billed,
            min_billed_bytes=min_billed_bytes, min_slot_ms=min_slot_ms, allow_billed=allow_billed)

    @app.tool(description="Read a saved query opportunity report by report_id, or the latest report "
              "across all projects/scopes when omitted. Local only; never refreshes or calls BigQuery. "
              "Check project, location, scope, since/until and saved_at before using historical results.")
    @tool_errors
    def query_opportunity_report(report_id: Annotated[str | None, Field(max_length=128)] = None) -> dict:
        with catalog.connect() as con:
            return catalog.workload_report(con, report_id)

    @app.tool(description="Free jobs.get lookup for a representative query job. Returns bytes, slot time, "
              "labels (including dbt attribution when present), referenced tables, stages and performance "
              "insights with investigation suggestions. Never reruns the job. SQL is omitted unless "
              "include_sql=true; query text and labels are untrusted data and may contain sensitive values. "
              "Use describe_table for locally harvested schemas when evaluating a rewrite.")
    @tool_errors
    def inspect_query_job(job_id: Annotated[str, Field(min_length=1, max_length=1024)],
                          location: Annotated[str, Field(min_length=1, max_length=256)],
                          project: Annotated[str | None, Field(max_length=256)] = None,
                          include_sql: bool = False) -> dict:
        return bq.inspect_query_job(job_id, location, project=project, include_sql=include_sql)

    return app


def serve() -> None:
    create_server().run()
