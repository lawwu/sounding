"""sounding: take a sounding before you dive.

  sounding harvest bigquery-public-data.thelook_ecommerce [more datasets...]
  sounding harvest --project my-project [--project other-project]
  sounding refresh [project.dataset ...] [--project project-id ...]
  sounding browse                     # local project/dataset hierarchy
  sounding show [text]                 # catalog only, no BigQuery call
  sounding describe <project.dataset.table>
  sounding dataset <project.dataset>   # dataset settings and its routines
  sounding routines [text]             # search functions and procedures
  sounding routine <project.dataset.routine>
  sounding column-costs <project.dataset.table>   # one free dry run per column
  sounding estimate "<sql>"            # free dry run
  sounding preview <project.dataset.table>        # free tabledata.list
  sounding cost-report [--hours 24]    # jobs this identity ran in the job project, with billed bytes
  sounding table-queries project.dataset.table [--days 30]  # free job history
  sounding query-opportunities --location US  # preview a capped workload scan
  sounding query-report [report_id]    # reopen a saved workload report, locally
  sounding inspect-job <job_id> --location US  # free execution details
  sounding mcp                         # stdio MCP server for agents
  sounding ui [--port 8765] [--open]   # local search UI
"""

from __future__ import annotations

import argparse
import json
import sys


def _main() -> None:
    p = argparse.ArgumentParser(prog="sounding", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    harvest_cmd = sub.add_parser("harvest", help="crawl datasets into the catalog (free calls only)")
    harvest_cmd.add_argument("datasets", nargs="*")
    refresh_cmd = sub.add_parser("refresh", help="refresh datasets or whole projects with progress (free metadata calls)")
    refresh_cmd.add_argument("datasets", nargs="*")
    for command in (harvest_cmd, refresh_cmd):
        command.add_argument("--project", action="append", default=[], help="discover and import a whole project; repeat for multiple projects")
        command.add_argument("--include-preview-rows", action="store_true",
                             help="save up to 10 rows / 256 KB per table; WARNING: can add a material delay and stores actual data locally")
    sub.add_parser("browse", help="local project and dataset hierarchy")
    show = sub.add_parser("show", help="list or search catalog tables")
    show.add_argument("q", nargs="?", default="")
    show.add_argument("--project", default="")
    show.add_argument("--dataset", default="")
    sub.add_parser("describe", help="one table from the catalog").add_argument("table")
    sub.add_parser("dataset", help="one dataset and its routines from the catalog").add_argument("dataset")
    sub.add_parser("model", help="one saved BigQuery model's metadata").add_argument("model")
    sub.add_parser("recommendations", help="cached project recommendations and insights").add_argument("project")
    sub.add_parser("routines", help="search harvested functions and procedures").add_argument("q", nargs="?", default="")
    sub.add_parser("routine", help="one function or procedure, with its definition").add_argument("routine")
    sub.add_parser("column-costs", help="per-column scan bytes via dry runs").add_argument("table")
    sub.add_parser("estimate", help="dry-run a query").add_argument("sql")
    sub.add_parser("preview", help="sample rows via tabledata.list").add_argument("table")
    report = sub.add_parser("cost-report", help="jobs run in the job project and what BigQuery billed (free)")
    report.add_argument("--hours", type=int, default=24, help="window to report, up to 4320 (180 days)")
    table_queries = sub.add_parser("table-queries", help="top individual query jobs referencing a table, via free jobs.list")
    table_queries.add_argument("table")
    table_queries.add_argument("--days", type=int, default=30)
    table_queries.add_argument("--project", help="job project to search; defaults to the configured job project")
    table_queries.add_argument("--scope", choices=["user", "project"], default="user")
    table_queries.add_argument("--rank-by", choices=["billed_bytes", "slot_ms"], default="billed_bytes")
    table_queries.add_argument("--limit", type=int, default=20)
    from . import workload
    opportunities = sub.add_parser("query-opportunities", help="find recurring costly queries; free preview unless --allow-billed")
    opportunities.add_argument("--location", required=True, help="BigQuery location, e.g. US or europe-west1")
    opportunities.add_argument("--project", help="project whose job history to scan; defaults to the job project")
    opportunities.add_argument("--scope", choices=["user", "project"], default="user")
    opportunities.add_argument("--hours", type=int, default=168, help="lookback, up to 4320 hours")
    opportunities.add_argument("--rank-by", choices=list(workload.RANKS), default="billed_bytes")
    opportunities.add_argument("--limit", type=int, default=20)
    opportunities.add_argument("--min-billed-bytes", type=int, default=workload.DEFAULT_MIN_BILLED_BYTES)
    opportunities.add_argument("--min-slot-ms", type=int, default=workload.DEFAULT_MIN_SLOT_MS)
    opportunities.add_argument("--max-bytes-billed", type=int, default=workload.DEFAULT_MAX_BYTES,
                               help="metadata scan byte cap: default 100 MiB, maximum 1 GiB")
    opportunities.add_argument("--allow-billed", action="store_true", help="execute the capped metadata scan and save the report")
    sub.add_parser("query-report", help="read a saved query opportunity report without calling BigQuery").add_argument("report_id", nargs="?")
    inspect = sub.add_parser("inspect-job", help="inspect query execution details using free jobs.get")
    inspect.add_argument("job_id")
    inspect.add_argument("--location", required=True)
    inspect.add_argument("--project")
    inspect.add_argument("--include-sql", action="store_true", help="include query text, which may contain sensitive literals")
    sub.add_parser("mcp", help="run the stdio MCP server")
    ui = sub.add_parser("ui", help="run the local search UI")
    ui.add_argument("--port", type=int, default=8765)
    ui.add_argument("--open", action="store_true", help="open it in your browser")
    a = p.parse_args()
    if a.cmd == "harvest" and not (a.datasets or a.project):
        p.error("harvest requires at least one dataset or --project PROJECT_ID")

    # Imports are deferred so `sounding --help` stays fast.
    if a.cmd == "mcp":
        from .mcp_server import serve
        return serve()
    if a.cmd == "ui":
        from .ui import serve as serve_ui
        return serve_ui(a.port, a.open)
    from . import bq, catalog
    from .progress import for_terminal
    if getattr(a, "include_preview_rows", False):
        print("Warning: including preview rows can add a material delay (one extra data-read request per native table). "
              "Up to 10 rows / 256 KB per table will be stored locally. Metadata is saved before row downloads finish.", file=sys.stderr)
    if a.cmd == "harvest" and not a.project:
        kwargs = {"include_preview_rows": True} if a.include_preview_rows else {}
        print(_receipt_line(bq.harvest(a.datasets, for_terminal("harvesting"), **kwargs)), file=sys.stderr)
    elif a.cmd in ("refresh", "harvest"):
        from .refresh import Job
        result = Job(a.datasets, a.project, for_terminal("refreshing"), include_preview_rows=a.include_preview_rows).run()
        print(json.dumps(result, indent=2))
        if result["status"] in ("partial", "failed"):
            raise SystemExit(2)
    elif a.cmd == "browse":
        with catalog.connect() as con:
            print(json.dumps(catalog.browse(con), indent=2))
    elif a.cmd == "show":
        offset = 0
        while offset is not None:
            with catalog.connect() as con:
                page = catalog.search(con, a.q, offset=offset, project=a.project, dataset=a.dataset)
            for r in page["items"]:
                size = "unknown" if r["bytes"] is None else f"{r['bytes'] / 1e9:.2f} GB"
                rows = "unknown" if r["rows"] is None else f"{r['rows']:,}"
                print(f"{size:>12}  {rows:>14}  {r['type']:<8} {r['table']}")
            offset = page["next_offset"]
    elif a.cmd == "describe":
        with catalog.connect() as con:
            result = catalog.describe(con, a.table)
        print(json.dumps(result, indent=2))
    elif a.cmd == "dataset":
        with catalog.connect() as con:
            print(json.dumps(catalog.describe_dataset(con, a.dataset), indent=2))
    elif a.cmd in ("model", "recommendations"):
        with catalog.connect() as con:
            result = catalog.describe_model(con, a.model) if a.cmd == "model" else catalog.recommendations(con, a.project)
        print(json.dumps(result, indent=2))
    elif a.cmd == "routines":
        offset = 0
        while offset is not None:
            with catalog.connect() as con:
                page = catalog.search_routines(con, a.q, offset=offset)
            for r in page["items"]:
                returns = f" -> {r['returns']}" if r["returns"] else ""
                print(f"{r['type'] or '':<22} {r['routine']}{r['signature']}{returns}")
            offset = page["next_offset"]
    elif a.cmd == "routine":
        with catalog.connect() as con:
            print(json.dumps(catalog.describe_routine(con, a.routine), indent=2))
    elif a.cmd == "column-costs":
        result = bq.column_costs(a.table, for_terminal("dry runs"))
        for col, b in sorted(result["columns"], key=lambda x: -(x[1] or 0)):
            size = "unknown" if b is None else f"{b / 1e6:.2f} MB"
            print(f"{size:>14}  {col}")
        for warning in result["warnings"]:
            print(f"note: {warning}", file=sys.stderr)
        print(_receipt_line(result["receipt"]), file=sys.stderr)
    elif a.cmd == "estimate":
        print(json.dumps(bq.dry_run(a.sql), indent=2))
    elif a.cmd == "preview":
        print(json.dumps(bq.preview(a.table), indent=2, default=str))
    elif a.cmd == "cost-report":
        print(json.dumps(bq.cost_report(a.hours), indent=2))
    elif a.cmd == "table-queries":
        print(json.dumps(bq.table_queries(a.table, a.days, project=a.project, scope=a.scope,
                                        rank_by=a.rank_by, limit=a.limit), indent=2))
    elif a.cmd == "query-opportunities":
        result = bq.query_opportunities(**{k: v for k, v in vars(a).items() if k != "cmd"})
        print(json.dumps(result, indent=2))
        if result["status"] in ("blocked", "failed"):
            raise SystemExit(2)
    elif a.cmd == "query-report":
        with catalog.connect() as con:
            print(json.dumps(catalog.workload_report(con, a.report_id), indent=2))
    elif a.cmd == "inspect-job":
        print(json.dumps(bq.inspect_query_job(a.job_id, a.location, project=a.project, include_sql=a.include_sql), indent=2))


def _receipt_line(r: dict) -> str:
    if "calls" in r:  # combined dry runs
        billed = "unknown" if r["billed_bytes"] is None else f"{r['billed_bytes']} bytes"
        verdict = "free" if r["free"] else "NOT CONFIRMED FREE"
        return (f"receipt: {r['calls']} dry runs, BigQuery billed {billed}, dry run confirmed: "
                f"{r['dry_run_confirmed']}, jobs created: {r['jobs_created']} ({verdict})")
    gets = ", ".join(f"{n} {call}" for call, n in r.get("get_calls", {}).items())
    return f"receipt: free API calls{' (' + gets + ')' if gets else ''}; no jobs created"


def main() -> None:
    from .errors import public_error
    try:
        _main()
    except Exception as error:
        message, status = public_error(error)
        if status >= 500:
            # The UI and MCP server keep internals out of responses; the CLI user is the
            # operator, so name the underlying error instead of pointing at stderr.
            message = f"{message.split('. See')[0]}: {type(error).__name__}: {error}"
        raise SystemExit(f"sounding: {message}") from None


if __name__ == "__main__":
    main()
