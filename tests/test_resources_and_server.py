"""Tests for the MCP resources (markdown rendering) and the FastMCP wiring."""

from __future__ import annotations

import asyncio

from nbb_mcp.catalog import Catalog
from nbb_mcp.models.catalog import (
    CatalogDimension,
    CodeRef,
    CommonQuery,
    EnrichedDataflow,
    TimeCoverage,
)
from nbb_mcp.resources.catalog_uri import (
    render_catalog_summary,
    render_category,
    render_dataflow,
)


def _make_catalog() -> Catalog:
    entries = [
        EnrichedDataflow(
            agency="BE2",
            id="DF_EXR",
            version="1.0",
            category="exchange_interest_rates",
            names={"en": "Reference exchange rates", "fr": "Taux de change"},
            dimensions=[
                CatalogDimension(
                    id="FREQ",
                    position=0,
                    codes=[CodeRef(id="D"), CodeRef(id="M")],
                    total_codes=2,
                ),
                CatalogDimension(
                    id="EXR_CURRENCY",
                    position=1,
                    codes=[CodeRef(id="USD"), CodeRef(id="GBP")],
                    total_codes=2,
                ),
            ],
            key_template="{FREQ}.{EXR_CURRENCY}",
            common_queries=[
                CommonQuery(label="Daily USD", key="D.USD", params={"lastNObservations": 10})
            ],
            time_coverage=TimeCoverage(),
        ),
        EnrichedDataflow(
            agency="BE2",
            id="DF_HICP_2025",
            version="1.0",
            category="prices",
            names={"en": "HICP"},
            time_coverage=TimeCoverage(),
        ),
    ]
    return Catalog(entries=entries)


# ---------------------------------------------------------------------------
# Markdown renderers
# ---------------------------------------------------------------------------


def test_render_catalog_summary_contains_counts():
    cat = _make_catalog()
    md = render_catalog_summary(cat)
    assert "# NBB Catalog" in md
    assert "Dataflows" in md
    assert "exchange_interest_rates" in md
    assert "DF_EXR" in md


def test_render_dataflow_includes_dimensions_and_queries():
    cat = _make_catalog()
    md = render_dataflow(cat.get("BE2", "DF_EXR"))
    assert "BE2/DF_EXR" in md
    assert "FREQ" in md
    assert "EXR_CURRENCY" in md
    assert "Daily USD" in md
    assert "Common queries" in md


def test_render_dataflow_multilang_names_listed():
    cat = _make_catalog()
    md = render_dataflow(cat.get("BE2", "DF_EXR"), language="fr")
    assert "Taux de change" in md
    assert "**fr**" in md
    assert "**en**" in md


def test_render_category_lists_matching_flows():
    cat = _make_catalog()
    md = render_category(cat, "prices")
    assert "prices" in md
    assert "DF_HICP_2025" in md
    assert "DF_EXR" not in md  # wrong category


def test_render_category_empty():
    cat = _make_catalog()
    md = render_category(cat, "nonexistent_category")
    assert "No dataflows" in md


# ---------------------------------------------------------------------------
# FastMCP server registration
# ---------------------------------------------------------------------------


def test_server_imports_and_registers_expected_tools():
    from nbb_mcp.server import mcp

    assert mcp.name == "nbb"
    tools = asyncio.run(mcp.list_tools())
    names = {t.name for t in tools}
    assert names == {
        "nbb_search",
        "nbb_describe",
        "nbb_query",
        "nbb_quick",
        "nbb_compare",
        "nbb_status",
    }


def test_server_registers_three_resource_uris():
    from nbb_mcp.server import mcp

    static = asyncio.run(mcp.list_resources())
    templates = asyncio.run(mcp.list_resource_templates())
    static_uris = {str(r.uri) for r in static}
    template_uris = {r.uriTemplate for r in templates}
    assert "nbb://catalog" in static_uris
    assert "nbb://dataflow/{agency}/{dataflow_id}" in template_uris
    assert "nbb://category/{category}" in template_uris


def test_server_tools_have_descriptions():
    from nbb_mcp.server import mcp

    tools = asyncio.run(mcp.list_tools())
    for t in tools:
        assert t.description, f"Tool {t.name} has no description"
        assert len(t.description) > 30
