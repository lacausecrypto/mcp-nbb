"""FastMCP server wiring for the NBB SDMX statistical API.

Registers 6 tools (``nbb_search``, ``nbb_describe``, ``nbb_query``, ``nbb_quick``,
``nbb_compare``, ``nbb_status``) and 3 resource URIs (``nbb://catalog``,
``nbb://dataflow/{agency}/{id}``, ``nbb://category/{name}``).

The :class:`Services` singleton is constructed in a FastMCP ``lifespan`` context
manager — this guarantees the HTTP client is opened on startup and closed on
shutdown while making tools reachable via :func:`get_services` with no explicit
injection.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.fastmcp import FastMCP

from .logging import configure_logging, get_logger
from .resources.catalog_uri import render_catalog_summary, render_category, render_dataflow
from .services import Services, close_services, get_services, reset_services
from .tools import compare as compare_tool
from .tools import describe as describe_tool
from .tools import query as query_tool
from .tools import quick as quick_tool
from .tools import search as search_tool
from .tools import status as status_tool

log = get_logger("nbb_mcp.server")


@asynccontextmanager
async def lifespan(_server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    svc = Services()
    reset_services(svc)
    log.info(
        "nbb.server.startup",
        dataflows=len(svc.catalog),
        categories=len(svc.catalog.categories()),
        api=svc.settings.api_base_url,
    )
    try:
        yield {"services": svc}
    finally:
        log.info("nbb.server.shutdown")
        await close_services()


mcp = FastMCP(
    name="nbb",
    instructions=(
        "Access the National Bank of Belgium statistical API (SDMX). Use nbb_search "
        "to discover dataflows, nbb_describe to inspect a flow's dimensions, and "
        "nbb_query or nbb_quick to fetch data. Prefer nbb_quick for common topics "
        "(exchange rates, inflation, GDP, unemployment, public debt) — it routes "
        "to the right dataflow automatically. Use nbb_compare to align 2-5 series "
        "on a common time index."
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    name="nbb_search",
    description=(
        "Fuzzy search across the local catalog of 221 NBB dataflows (no API call). "
        "Filters by category and agency. Returns ranked hits with dimensions and "
        "example queries."
    ),
)
async def nbb_search(
    query: str,
    category: str = "all",
    agency: str = "all",
    language: str = "en",
    include_non_final: bool = False,
    limit: int = 10,
) -> dict[str, Any]:
    return await search_tool.run(
        query,
        category=category,
        agency=agency,
        language=language,
        include_non_final=include_non_final,
        limit=limit,
    )


@mcp.tool(
    name="nbb_describe",
    description=(
        "Return the full enriched fiche for a dataflow (dimensions, codelists, "
        "multilingual names, common queries). Reads from the local catalog by "
        "default; pass force_refresh=True to revalidate against the live API."
    ),
)
async def nbb_describe(
    dataflow_id: str,
    agency: str = "BE2",
    language: str = "en",
    include_codes: bool = True,
    force_refresh: bool = False,
) -> dict[str, Any]:
    return await describe_tool.run(
        dataflow_id,
        agency=agency,
        language=language,
        include_codes=include_codes,
        force_refresh=force_refresh,
    )


@mcp.tool(
    name="nbb_query",
    description=(
        "Generic SDMX data fetch. Specify either 'key' (raw SDMX, e.g. 'D.USD') or "
        "'filters' ({'FREQ':'D','EXR_CURRENCY':'USD'}). Supports start_period, "
        "end_period, lastNObservations and a max_observations token budget."
    ),
)
async def nbb_query(
    dataflow_id: str,
    agency: str = "BE2",
    version: str | None = None,
    key: str | None = None,
    filters: dict[str, Any] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    first_n_observations: int | None = None,
    max_observations: int = 200,
    format: str = "summary",
) -> dict[str, Any]:
    return await query_tool.run(
        dataflow_id,
        agency=agency,
        version=version,
        key=key,
        filters=filters,
        start_period=start_period,
        end_period=end_period,
        last_n_observations=last_n_observations,
        first_n_observations=first_n_observations,
        max_observations=max_observations,
        format=format,
    )


@mcp.tool(
    name="nbb_quick",
    description=(
        "Topic-based shortcut for common queries (exchange_rate, inflation_hicp, "
        "gdp, unemployment_rate, government_debt, policy_rate, mortgage_rate, "
        "consumer_confidence, trade_balance, …). Routes to the right dataflow "
        "automatically. Use nbb_quick('exchange_rate', currency='USD') for FX."
    ),
)
async def nbb_quick(
    topic: str,
    currency: str | None = None,
    frequency: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    max_observations: int = 200,
    format: str = "summary",
) -> dict[str, Any]:
    return await quick_tool.run(
        topic,
        currency=currency,
        frequency=frequency,
        start_period=start_period,
        end_period=end_period,
        last_n_observations=last_n_observations,
        max_observations=max_observations,
        format=format,
    )


@mcp.tool(
    name="nbb_compare",
    description=(
        "Align 2-5 SDMX series on a common time index. Each series is specified "
        "as {dataflow_id, agency?, key? | filters?, label?}. Downsamples finer "
        "frequencies to the coarsest using closing aggregation."
    ),
)
async def nbb_compare(
    series: list[dict[str, Any]],
    start_period: str | None = None,
    end_period: str | None = None,
    frequency: str | None = None,
) -> dict[str, Any]:
    return await compare_tool.run(
        series,
        start_period=start_period,
        end_period=end_period,
        frequency=frequency,
    )


@mcp.tool(
    name="nbb_status",
    description="Diagnostic snapshot: catalog stats, HTTP cache footprint, API config.",
)
async def nbb_status() -> dict[str, Any]:
    return await status_tool.run()


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource(
    "nbb://catalog",
    name="NBB Catalog",
    description="Summary of the 221 NBB dataflows organised by category.",
    mime_type="text/markdown",
)
async def catalog_resource() -> str:
    return render_catalog_summary(get_services().catalog)


@mcp.resource(
    "nbb://dataflow/{agency}/{dataflow_id}",
    name="NBB Dataflow fiche",
    description="Detailed enriched fiche for a single dataflow.",
    mime_type="text/markdown",
)
async def dataflow_resource(agency: str, dataflow_id: str) -> str:
    entry = get_services().catalog.get(agency, dataflow_id)
    return render_dataflow(entry)


@mcp.resource(
    "nbb://category/{category}",
    name="NBB Category",
    description="Dataflows within a category.",
    mime_type="text/markdown",
)
async def category_resource(category: str) -> str:
    return render_category(get_services().catalog, category)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    configure_logging()
    mcp.run()  # default transport: stdio


if __name__ == "__main__":
    main()
