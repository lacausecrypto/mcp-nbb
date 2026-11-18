"""MCP server wiring for the NBB SDMX statistical API (mcp 2.x ``MCPServer``).

Registers 6 tools (``nbb_search``, ``nbb_describe``, ``nbb_query``, ``nbb_quick``,
``nbb_compare``, ``nbb_status``) and 3 resource URIs (``nbb://catalog``,
``nbb://dataflow/{agency}/{id}``, ``nbb://category/{name}``).

The :class:`Services` singleton is constructed in the server ``lifespan`` — the
HTTP client is opened on startup and closed on shutdown, and tools reach it via
:func:`get_services` with no explicit injection.

Error contract: mcp 2.x forwards the text of :class:`ToolError` /
:class:`ResourceError` to the client and replaces any other exception with a
generic "Error executing tool" message. Every :class:`NBBError` is therefore
converted at the boundary so the model sees the error code and the actionable
``details`` (valid dimensions, codes, key template).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ResourceNotFoundError, ToolError
from mcp.types import ToolAnnotations

from .config import get_settings
from .logging import configure_logging, get_logger
from .models.errors import NBBCatalogError, NBBError
from .resources.catalog_uri import render_catalog_summary, render_category, render_dataflow
from .services import Services, close_services, get_services, reset_services
from .tools import compare as compare_tool
from .tools import describe as describe_tool
from .tools import query as query_tool
from .tools import quick as quick_tool
from .tools import search as search_tool
from .tools import status as status_tool

log = get_logger("nbb_mcp.server")

_T = TypeVar("_T")

# Catalogue-only tools never leave the process; data tools hit the NBB API.
_LOCAL = ToolAnnotations(read_only_hint=True, open_world_hint=False, idempotent_hint=True)
_REMOTE = ToolAnnotations(read_only_hint=True, open_world_hint=True, idempotent_hint=True)


async def _tool_call(coro: Awaitable[_T]) -> _T:
    try:
        return await coro
    except NBBError as exc:
        raise ToolError(str(exc)) from exc


def _resource_call(fn: Callable[[], _T]) -> _T:
    try:
        return fn()
    except NBBCatalogError as exc:
        if exc.code in {"UNKNOWN_FLOW", "UNKNOWN_CATEGORY"}:
            raise ResourceNotFoundError(str(exc)) from exc
        raise ResourceError(str(exc)) from exc
    except NBBError as exc:
        raise ResourceError(str(exc)) from exc


@asynccontextmanager
async def lifespan(_server: MCPServer[Any]) -> AsyncIterator[dict[str, Any]]:
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


mcp = MCPServer(
    name="nbb",
    instructions=(
        "Access the National Bank of Belgium statistical API (SDMX). Use nbb_search "
        "to discover dataflows, nbb_describe to inspect a flow's dimensions, and "
        "nbb_query or nbb_quick to fetch data. Prefer nbb_quick for common topics "
        "(exchange rates, inflation, GDP, unemployment, public debt) — it routes "
        "to the right dataflow and headline series automatically. Use nbb_compare "
        "to align 2-5 series on a common time index. Errors carry a [CODE] and a "
        "'details' JSON listing valid dimensions/codes — use them to fix the call."
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    name="nbb_search",
    description=(
        "Fuzzy search across the local catalogue of NBB dataflows (no API call). "
        "Filters by category and agency. Returns ranked hits with dimensions, "
        "available frequencies, time coverage and example queries."
    ),
    annotations=_LOCAL,
)
async def nbb_search(
    query: str,
    category: str = "all",
    agency: str = "all",
    language: str = "en",
    include_non_final: bool = False,
    limit: int = 10,
) -> dict[str, Any]:
    return await _tool_call(
        search_tool.run(
            query,
            category=category,
            agency=agency,
            language=language,
            include_non_final=include_non_final,
            limit=limit,
        )
    )


@mcp.tool(
    name="nbb_describe",
    description=(
        "Return the full enriched fiche for a dataflow (dimensions with the codes "
        "that actually hold data, multilingual names, frequencies, time coverage, "
        "common queries). Reads from the local catalogue by default; pass "
        "force_refresh=True to revalidate the structure against the live API."
    ),
    annotations=_LOCAL,
)
async def nbb_describe(
    dataflow_id: str,
    agency: str = "BE2",
    language: str = "en",
    include_codes: bool = True,
    force_refresh: bool = False,
) -> dict[str, Any]:
    return await _tool_call(
        describe_tool.run(
            dataflow_id,
            agency=agency,
            language=language,
            include_codes=include_codes,
            force_refresh=force_refresh,
        )
    )


@mcp.tool(
    name="nbb_query",
    description=(
        "Generic SDMX data fetch. Specify either 'key' (raw SDMX, e.g. 'D.USD') or "
        "'filters' ({'FREQ':'D','EXR_CURRENCY':'USD'}; a list value means OR). "
        "Supports start_period, end_period, last/first_n_observations (per series). "
        "max_observations caps returned rows and max_series caps per-series stats "
        "to keep the response within the model's context budget."
    ),
    annotations=_REMOTE,
)
async def nbb_query(
    dataflow_id: str,
    agency: str = "BE2",
    version: str | None = None,
    key: str | None = None,
    filters: dict[str, str | list[str]] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    first_n_observations: int | None = None,
    max_observations: int = 200,
    max_series: int = 20,
    format: str = "summary",
) -> dict[str, Any]:
    return await _tool_call(
        query_tool.run(
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
            max_series=max_series,
            format=format,
        )
    )


@mcp.tool(
    name="nbb_quick",
    description=(
        "Topic-based shortcut for common queries (exchange_rate, inflation_hicp, "
        "gdp, unemployment_rate, government_debt, policy_rate, mortgage_rate, "
        "consumer_confidence, trade_balance, …). Routes to the right dataflow "
        "and headline series. 'frequency' (A/S/Q/M/D) applies to every topic; "
        "'currency' only to exchange_rate, e.g. nbb_quick('exchange_rate', currency='USD')."
    ),
    annotations=_REMOTE,
)
async def nbb_quick(
    topic: str,
    currency: str | None = None,
    frequency: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    max_observations: int = 200,
    max_series: int = 20,
    format: str = "summary",
) -> dict[str, Any]:
    return await _tool_call(
        quick_tool.run(
            topic,
            currency=currency,
            frequency=frequency,
            start_period=start_period,
            end_period=end_period,
            last_n_observations=last_n_observations,
            max_observations=max_observations,
            max_series=max_series,
            format=format,
        )
    )


@mcp.tool(
    name="nbb_compare",
    description=(
        "Align 2-5 SDMX series on a common time index. Each series is specified "
        "as {dataflow_id, agency?, key? | filters?, label?} and must resolve to a "
        "single series. Downsamples finer frequencies to the coarsest using "
        "closing (last-of-period) aggregation."
    ),
    annotations=_REMOTE,
)
async def nbb_compare(
    series: list[dict[str, Any]],
    start_period: str | None = None,
    end_period: str | None = None,
    frequency: str | None = None,
    max_periods: int = 200,
) -> dict[str, Any]:
    return await _tool_call(
        compare_tool.run(
            series,
            start_period=start_period,
            end_period=end_period,
            frequency=frequency,
            max_periods=max_periods,
        )
    )


@mcp.tool(
    name="nbb_status",
    description="Diagnostic snapshot: catalogue stats, HTTP cache footprint, API config and usage.",
    annotations=_LOCAL,
)
async def nbb_status() -> dict[str, Any]:
    return await _tool_call(status_tool.run())


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource(
    "nbb://catalog",
    name="NBB Catalog",
    description="Summary of the NBB dataflows organised by category.",
    mime_type="text/markdown",
)
async def catalog_resource() -> str:
    return _resource_call(lambda: render_catalog_summary(get_services().catalog))


@mcp.resource(
    "nbb://dataflow/{agency}/{dataflow_id}",
    name="NBB Dataflow fiche",
    description="Detailed enriched fiche for a single dataflow.",
    mime_type="text/markdown",
)
async def dataflow_resource(agency: str, dataflow_id: str) -> str:
    return _resource_call(lambda: render_dataflow(get_services().catalog.get(agency, dataflow_id)))


@mcp.resource(
    "nbb://category/{category}",
    name="NBB Category",
    description="Dataflows within a category.",
    mime_type="text/markdown",
)
async def category_resource(category: str) -> str:
    return _resource_call(lambda: render_category(get_services().catalog, category))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    mcp.run()  # default transport: stdio


if __name__ == "__main__":
    main()
