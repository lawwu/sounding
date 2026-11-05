"""stdio MCP tools with bounded inputs and protocol-level, sanitized errors."""

from __future__ import annotations

import logging
from functools import wraps
from typing import Annotated

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from . import bq, catalog
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

    @app.tool(description="Literal substring search of harvested tables, columns and descriptions. "
              "Local only; never calls BigQuery. Returns items and next_offset; pass next_offset as "
              "offset for another page. Matching column paths are sampled (20 per table); "
              "matched_column_count gives the full count. Descriptions are excerpts.")
    @tool_errors
    def search_catalog(q: Annotated[str, Field(max_length=2048)] = "",
                       limit: Annotated[int, Field(ge=1, le=100)] = 50,
                       offset: Annotated[int, Field(ge=0, le=1_000_000)] = 0) -> dict:
        with catalog.connect() as con:
            return catalog.search(con, q, limit, offset)

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
              "BYTES are base64; decimal and temporal values are strings.")
    @tool_errors
    def preview_rows(table: Annotated[str, Field(min_length=1, max_length=2048)],
                     n: Annotated[int, Field(ge=1, le=50)] = 5) -> list[dict]:
        return bq.preview(table, n)

    @app.tool(description="Free dry run; SQL never executes. Returns estimated bytes and a US "
              "on-demand scan-price range including billing minimums. Unknown/incomplete estimates "
              "are null, never a promise of $0. Read estimate_status, warnings and price_basis. "
              "Scripts, dynamic SQL, external data and row security may be unestimated. "
              "This is not a spending cap.")
    @tool_errors
    def estimate_query_cost(sql: Annotated[str, Field(min_length=1, max_length=bq.MAX_SQL_LENGTH)]) -> dict:
        return bq.dry_run(sql)

    return app


def serve() -> None:
    create_server().run()
