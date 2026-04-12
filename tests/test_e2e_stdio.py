"""End-to-end tests: spawn the FastMCP server via stdio and speak the protocol.

These tests cover the offline-friendly tools only (``nbb_search``,
``nbb_describe``, ``nbb_status``) and the three static/templated resources.
Live data tools (``nbb_query``, ``nbb_quick``, ``nbb_compare``) are skipped
here because they require the real NBB API — they are exercised by the
in-process integration suite in ``test_tools.py`` with a ``MockTransport``.

The subprocess uses the bundled catalogue, so tests are deterministic and
hermetic as long as ``src/nbb_mcp/data/catalog/`` has been populated.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

CATALOG = Path(__file__).resolve().parent.parent / "src" / "nbb_mcp" / "data" / "catalog"

pytestmark = [
    pytest.mark.skipif(
        not CATALOG.is_dir() or not any(CATALOG.glob("BE2_*.json")),
        reason="Bundled catalogue not built (run `mcp-nbb-build-catalog`).",
    ),
    pytest.mark.e2e,
]


def _server_params() -> StdioServerParameters:
    """Build stdio parameters with a cross-platform environment.

    We inherit the parent process env (so PATH, PYTHONPATH, SystemRoot… all
    propagate to the subprocess on Windows) and override only a few MCP-specific
    knobs needed to make tests deterministic.
    """
    env = os.environ.copy()
    env.update(
        {
            "NBB_LOG_LEVEL": "WARNING",
            "NBB_LOG_FORMAT": "console",
            "NBB_HTTP_CACHE_ENABLED": "false",
        }
    )
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "nbb_mcp.server"],
        env=env,
    )


def _text_payload(result) -> str:
    """Extract the first text block from an MCP tool call result."""
    for block in result.content:
        text = getattr(block, "text", None)
        if text is not None:
            return text
    return ""


async def test_e2e_initialize_and_list_tools():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            assert init.serverInfo.name == "nbb"

            tools = await session.list_tools()
            names = {t.name for t in tools.tools}
            assert names == {
                "nbb_search",
                "nbb_describe",
                "nbb_query",
                "nbb_quick",
                "nbb_compare",
                "nbb_status",
            }
            for t in tools.tools:
                assert t.description
                assert len(t.description) > 30


async def test_e2e_list_resources_and_templates():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            resources = await session.list_resources()
            static_uris = {str(r.uri) for r in resources.resources}
            assert "nbb://catalog" in static_uris

            templates = await session.list_resource_templates()
            template_uris = {r.uriTemplate for r in templates.resourceTemplates}
            assert "nbb://dataflow/{agency}/{dataflow_id}" in template_uris
            assert "nbb://category/{category}" in template_uris


async def test_e2e_call_nbb_status():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            result = await session.call_tool("nbb_status", {})
            assert not result.isError
            payload = json.loads(_text_payload(result))
            assert payload["catalog"]["dataflow_count"] == 221
            assert "exchange_interest_rates" in payload["catalog"]["categories"]
            assert payload["api"]["base_url"].startswith("https://")


async def test_e2e_call_nbb_search():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "nbb_search",
                {"query": "exchange rate of the euro", "limit": 5},
            )
            assert not result.isError
            payload = json.loads(_text_payload(result))
            assert payload["count"] >= 1
            assert any(hit["id"] == "DF_EXR" for hit in payload["results"])


async def test_e2e_call_nbb_describe():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "nbb_describe",
                {"dataflow_id": "DF_EXR", "agency": "BE2", "include_codes": False},
            )
            assert not result.isError
            payload = json.loads(_text_payload(result))
            assert payload["id"] == "DF_EXR"
            assert payload["key_template"] == "{FREQ}.{EXR_CURRENCY}"
            assert {d["id"] for d in payload["dimensions"]} >= {"FREQ", "EXR_CURRENCY"}


async def test_e2e_read_catalog_resource():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            resp = await session.read_resource("nbb://catalog")
            assert resp.contents
            first = resp.contents[0]
            text = getattr(first, "text", "")
            assert "NBB Catalog" in text
            assert "exchange_interest_rates" in text


async def test_e2e_read_dataflow_resource_templated():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            resp = await session.read_resource("nbb://dataflow/BE2/DF_EXR")
            text = getattr(resp.contents[0], "text", "")
            assert "BE2/DF_EXR" in text
            assert "FREQ" in text
            assert "EXR_CURRENCY" in text


async def test_e2e_tool_error_surfaces_as_structured_error():
    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "nbb_describe",
                {"dataflow_id": "DF_DOES_NOT_EXIST", "agency": "BE2"},
            )
            # FastMCP reports tool exceptions via isError=True or error content.
            text = _text_payload(result)
            assert result.isError or "UNKNOWN_FLOW" in text or "Unknown dataflow" in text
