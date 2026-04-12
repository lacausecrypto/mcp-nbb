"""Benchmark the 6 NBB MCP tools end-to-end (p50/p95 latency per tool).

Runs each tool N times against the real NBB API (with the persistent disk
cache enabled, so only the first call of each pair actually hits the network)
and prints a latency table. Meant to run locally, not in CI.

Usage::

    python -m nbb_mcp.scripts.bench              # 20 iterations per tool
    python -m nbb_mcp.scripts.bench --iter 50    # more precision
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ..logging import configure_logging
from ..services import Services, close_services, reset_services
from ..tools import compare as compare_tool
from ..tools import describe as describe_tool
from ..tools import query as query_tool
from ..tools import quick as quick_tool
from ..tools import search as search_tool
from ..tools import status as status_tool


async def _time_once(fn: Callable[[], Awaitable[Any]]) -> float:
    t0 = time.perf_counter()
    await fn()
    return (time.perf_counter() - t0) * 1000.0


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    k = (len(sorted_v) - 1) * q
    f = int(k)
    c = min(f + 1, len(sorted_v) - 1)
    return sorted_v[f] + (sorted_v[c] - sorted_v[f]) * (k - f)


async def run(iterations: int = 20) -> list[dict[str, Any]]:
    svc = Services()
    reset_services(svc)

    scenarios: dict[str, Callable[[], Awaitable[Any]]] = {
        "nbb_status": lambda: status_tool.run(),
        "nbb_search": lambda: search_tool.run("exchange rate", limit=5),
        "nbb_describe": lambda: describe_tool.run("DF_EXR", include_codes=False),
        "nbb_query (D.USD lastN=10)": lambda: query_tool.run(
            "DF_EXR", filters={"FREQ": "D", "EXR_CURRENCY": "USD"}, last_n_observations=10
        ),
        "nbb_quick (exchange_rate USD)": lambda: quick_tool.run(
            "exchange_rate", currency="USD", frequency="D", last_n_observations=10
        ),
        "nbb_compare (USD vs GBP)": lambda: compare_tool.run(
            [
                {"dataflow_id": "DF_EXR", "label": "USD", "filters": {"FREQ": "M", "EXR_CURRENCY": "USD"}},
                {"dataflow_id": "DF_EXR", "label": "GBP", "filters": {"FREQ": "M", "EXR_CURRENCY": "GBP"}},
            ],
            start_period="2024-01",
            end_period="2024-12",
            frequency="M",
        ),
    }

    rows: list[dict[str, Any]] = []
    try:
        # Warm up the cache with one pass.
        for fn in scenarios.values():
            try:
                await fn()
            except Exception:
                pass

        for label, fn in scenarios.items():
            samples: list[float] = []
            for _ in range(iterations):
                samples.append(await _time_once(fn))
            rows.append(
                {
                    "tool": label,
                    "n": iterations,
                    "p50_ms": round(_percentile(samples, 0.5), 2),
                    "p95_ms": round(_percentile(samples, 0.95), 2),
                    "min_ms": round(min(samples), 2),
                    "max_ms": round(max(samples), 2),
                    "mean_ms": round(statistics.mean(samples), 2),
                }
            )
    finally:
        await close_services()

    return rows


def _print_table(rows: list[dict[str, Any]]) -> None:
    cols = ["tool", "n", "p50_ms", "p95_ms", "mean_ms", "min_ms", "max_ms"]
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark nbb-mcp tools")
    parser.add_argument("--iter", type=int, default=20)
    args = parser.parse_args()

    configure_logging(level="WARNING", fmt="console")
    try:
        rows = asyncio.run(run(iterations=args.iter))
    except KeyboardInterrupt:  # pragma: no cover
        sys.exit(130)
    _print_table(rows)


if __name__ == "__main__":
    main()
