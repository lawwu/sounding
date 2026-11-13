import asyncio
import os
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.mcpserver.exceptions import ToolError

from sounding import bq, catalog, workload
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
        scan = schemas["query_opportunities"]
        assert scan["required"] == ["location"]
        assert scan["properties"]["allow_billed"]["default"] is False
        assert scan["properties"]["max_bytes_billed"]["maximum"] == workload.MAX_BYTES
        assert scan["properties"]["max_bytes_billed"]["default"] == workload.DEFAULT_MAX_BYTES
        assert schemas["inspect_query_job"]["properties"]["include_sql"]["default"] is False
        assert "query_opportunity_report" in schemas
        for n in [-1,0,51]:
            with pytest.raises(ToolError): await app.call_tool("preview_rows",{"table":"p.d.t","n":n})
        with pytest.raises(ToolError,match="latest catalog"):
            await app.call_tool("describe_table",{"table":"p.d.missing"})
        with pytest.raises(ToolError,match="not in the catalog"):
            await app.call_tool("describe_dataset",{"dataset":"p.missing"})
    asyncio.run(check())


def test_monitoring_tools_forward_opt_in_and_read_saved_results(monkeypatch):
    seen = []
    def scan(location, **kwargs):
        seen.append((location, kwargs))
        return {"status": "complete" if kwargs["allow_billed"] else "preview"}
    monkeypatch.setattr(bq, "query_opportunities", scan)
    saved = catalog.save_workload_report({"status": "complete", "project": "saved-project"})
    async def check():
        app = create_server()
        await app.call_tool("query_opportunities", {"location": "US"})
        await app.call_tool("query_opportunities", {"location": "EU", "allow_billed": True, "rank_by": "slot_ms"})
        result = await app.call_tool("query_opportunity_report", {"report_id": saved["report_id"]})
        assert "saved-project" in str(result)
        with pytest.raises(ToolError):
            await app.call_tool("query_opportunities", {"location": "US", "max_bytes_billed": workload.MAX_BYTES + 1})
    asyncio.run(check())
    assert seen[0][0] == "US" and seen[0][1]["allow_billed"] is False
    assert seen[1][0] == "EU" and seen[1][1]["allow_billed"] is True


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


def test_browse_refresh_and_history_tools(monkeypatch, seed):
    from sounding import refresh
    seed()
    started = []
    monkeypatch.setattr(refresh.Manager, 'start', lambda self,datasets,projects,**options: started.append((datasets,projects)) or {'status':'running'})
    async def check():
        app = create_server()
        schemas = {t.name:t.input_schema for t in await app.list_tools()}
        assert schemas['table_query_history']['properties']['days']['default'] == 30
        assert schemas['refresh_metadata']['properties']['projects']['anyOf'][0]['maxItems'] == 25
        assert 'p.d' in str(await app.call_tool('browse_catalog', {}))
        await app.call_tool('refresh_metadata', {'projects':['p','q']})
        assert 'idle' in str(await app.call_tool('refresh_status', {}))
        assert 'p.d.t' in str(await app.call_tool('search_catalog', {'dataset':'p.d'}))
    asyncio.run(check())
    assert started == [(None,['p','q'])]
