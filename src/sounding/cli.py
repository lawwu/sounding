"""sounding: take a sounding before you dive.

  sounding harvest bigquery-public-data.thelook_ecommerce [more datasets...]
  sounding show [text]                 # catalog only, no BigQuery call
  sounding describe <project.dataset.table>
  sounding column-costs <project.dataset.table>   # one free dry run per column
  sounding estimate "<sql>"            # free dry run
  sounding preview <project.dataset.table>        # free tabledata.list
  sounding mcp                         # stdio MCP server for agents
  sounding ui [--port 8765] [--open]   # local search UI
"""

from __future__ import annotations

import argparse
import json


def _main() -> None:
    p = argparse.ArgumentParser(prog="sounding", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("harvest", help="crawl datasets into the catalog (free calls only)").add_argument("datasets", nargs="+")
    sub.add_parser("show", help="list or search catalog tables").add_argument("q", nargs="?", default="")
    sub.add_parser("describe", help="one table from the catalog").add_argument("table")
    sub.add_parser("column-costs", help="per-column scan bytes via dry runs").add_argument("table")
    sub.add_parser("estimate", help="dry-run a query").add_argument("sql")
    sub.add_parser("preview", help="sample rows via tabledata.list").add_argument("table")
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
        bq.harvest(a.datasets)
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
    elif a.cmd == "column-costs":
        for col, b in sorted(bq.column_costs(a.table), key=lambda x: -(x[1] or 0)):
            size = "unknown" if b is None else f"{b / 1e6:.2f} MB"
            print(f"{size:>14}  {col}")
    elif a.cmd == "estimate":
        print(json.dumps(bq.dry_run(a.sql), indent=2))
    elif a.cmd == "preview":
        print(json.dumps(bq.preview(a.table), indent=2, default=str))


def main() -> None:
    from .errors import public_error
    try:
        _main()
    except Exception as error:
        message, _ = public_error(error)
        raise SystemExit(f"sounding: {message}") from None


if __name__ == "__main__":
    main()
