"""stdio MCP server over the catalog. Tools are grouped by what they can cost."""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer

from . import bq, catalog


def serve() -> None:
    app = MCPServer("sounding")

    # Catalog only: never calls BigQuery.
    @app.tool(description="Search harvested BigQuery tables by name, column path or description. "
                          "Never calls BigQuery.")
    def search_catalog(q: str = "") -> list[dict]:
        return catalog.search(catalog.connect(), q)

    @app.tool(description="Schema, size, freshness, partitioning and clustering of one table "
                          "(project.dataset.table) from the catalog. Never calls BigQuery.")
    def describe_table(table: str) -> dict:
        return catalog.describe(catalog.connect(), table)

    # Free BigQuery calls.
    @app.tool(description="Up to 50 sample rows via tabledata.list. Free; tables only, not views.")
    def preview_rows(table: str, n: int = 5) -> list[dict]:
        return bq.preview(table, min(n, 50))

    @app.tool(description="Dry-run a query: bytes it would scan, on-demand $ estimate and the tables "
                          "it reads. Free; nothing executes. Use before suggesting any query.")
    def estimate_query_cost(sql: str) -> dict:
        return bq.dry_run(sql)

    app.run()
