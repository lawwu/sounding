import asyncio
import os
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.mcpserver.exceptions import ToolError

from sounding import bq, catalog
from sounding.mcp_server import create_server


def test_tool_schemas_and_bounds(monkeypatch):
    async def check():
        app=create_server()
        schemas={t.name:t.input_schema for t in await app.list_tools()}
        assert {"describe_dataset","search_routines","describe_routine","cost_report"} <= set(schemas)
        assert schemas["cost_report"]["properties"]["hours"]["maximum"]==bq.COST_REPORT_MAX_HOURS
        assert schemas["search_routines"]["properties"]["limit"]["maximum"]==100
        n=schemas["preview_rows"]["properties"]["n"]
        assert n["minimum"]==1 and n["maximum"]==50
        assert schemas["search_catalog"]["properties"]["limit"]["maximum"]==100
        assert schemas["estimate_query_cost"]["properties"]["sql"]["maxLength"]==bq.MAX_SQL_LENGTH
        for n in [-1,0,51]:
            with pytest.raises(ToolError): await app.call_tool("preview_rows",{"table":"p.d.t","n":n})
        with pytest.raises(ToolError,match="latest catalog"):
            await app.call_tool("describe_table",{"table":"p.d.missing"})
        with pytest.raises(ToolError,match="not in the catalog"):
            await app.call_tool("describe_dataset",{"dataset":"p.missing"})
    asyncio.run(check())


def test_missing_table_is_protocol_error_over_stdio():
    async def check():
        params=StdioServerParameters(command=sys.executable,args=["-m","sounding.cli","mcp"],
                                     env=dict(os.environ,SOUNDING_CATALOG=str(catalog.CATALOG)))
        async with stdio_client(params) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize()
                result=await session.call_tool("describe_table",{"table":"p.d.missing"})
                assert result.is_error
                assert "latest catalog" in result.content[0].text
                result=await session.call_tool("search_catalog",{})
                assert not result.is_error
    asyncio.run(check())
