"""sounding: take a sounding before you dive.

  sounding harvest bigquery-public-data.thelook_ecommerce [more datasets...]
  sounding show [text]                 # catalog only, no BigQuery call
  sounding describe <project.dataset.table>
  sounding dataset <project.dataset>   # dataset settings and its routines
  sounding routines [text]             # search functions and procedures
  sounding routine <project.dataset.routine>
  sounding column-costs <project.dataset.table>   # one free dry run per column
  sounding estimate "<sql>"            # free dry run
  sounding preview <project.dataset.table>        # free tabledata.list
  sounding cost-report [--hours 24]    # jobs this identity ran in the job project, with billed bytes
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
    sub.add_parser("harvest", help="crawl datasets into the catalog (free calls only)").add_argument("datasets", nargs="+")
    sub.add_parser("show", help="list or search catalog tables").add_argument("q", nargs="?", default="")
    sub.add_parser("describe", help="one table from the catalog").add_argument("table")
    sub.add_parser("dataset", help="one dataset and its routines from the catalog").add_argument("dataset")
    sub.add_parser("routines", help="search harvested functions and procedures").add_argument("q", nargs="?", default="")
    sub.add_parser("routine", help="one function or procedure, with its definition").add_argument("routine")
    sub.add_parser("column-costs", help="per-column scan bytes via dry runs").add_argument("table")
    sub.add_parser("estimate", help="dry-run a query").add_argument("sql")
    sub.add_parser("preview", help="sample rows via tabledata.list").add_argument("table")
    report = sub.add_parser("cost-report", help="jobs run in the job project and what BigQuery billed (free)")
    report.add_argument("--hours", type=int, default=24, help="window to report, up to 4320 (180 days)")
    sub.add_parser("mcp", help="run the stdio MCP server")
    ui = sub.add_parser("ui", help="run the local search UI")
    ui.add_argument("--port", type=int, default=8765)
    ui.add_argument("--open", action="store_true", help="open it in your browser")
    a = p.parse_args()

    # Imports are deferred so `sounding --help` stays fast.
    if a.cmd == "mcp":
        from .mcp_server import serve
        return serve()
    if a.cmd == "ui":
        from .ui import serve as serve_ui
        return serve_ui(a.port, a.open)
    from . import bq, catalog
    if a.cmd == "harvest":
        print(_receipt_line(bq.harvest(a.datasets)), file=sys.stderr)
    elif a.cmd == "show":
        offset = 0
        while offset is not None:
            with catalog.connect() as con:
                page = catalog.search(con, a.q, offset=offset)
            for r in page["items"]:
                print(f"{(r['bytes'] or 0) / 1e9:9.2f} GB  {r['rows'] or 0:>14,}  {r['type']:<6} {r['table']}")
            offset = page["next_offset"]
    elif a.cmd == "describe":
        with catalog.connect() as con:
            result = catalog.describe(con, a.table)
        print(json.dumps(result, indent=2))
    elif a.cmd == "dataset":
        with catalog.connect() as con:
            print(json.dumps(catalog.describe_dataset(con, a.dataset), indent=2))
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
        result = bq.column_costs(a.table)
        for col, b in sorted(result["columns"], key=lambda x: -(x[1] or 0)):
            size = "unknown" if b is None else f"{b / 1e6:.2f} MB"
            print(f"{size:>14}  {col}")
        print(_receipt_line(result["receipt"]), file=sys.stderr)
    elif a.cmd == "estimate":
        print(json.dumps(bq.dry_run(a.sql), indent=2))
    elif a.cmd == "preview":
        print(json.dumps(bq.preview(a.table), indent=2, default=str))
    elif a.cmd == "cost-report":
        print(json.dumps(bq.cost_report(a.hours), indent=2))


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
        message, _ = public_error(error)
        raise SystemExit(f"sounding: {message}") from None


if __name__ == "__main__":
    main()
