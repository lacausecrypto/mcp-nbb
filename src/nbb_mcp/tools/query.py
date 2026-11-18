"""``nbb_query`` — generic SDMX data fetcher with filter→key resolution."""

from __future__ import annotations

from typing import Any

from ..formatter import DEFAULT_MAX_SERIES, format_data_message, validate_format
from ..models.errors import NBBValidationError
from ..parsers.sdmx_json_v2 import parse_data_message
from ..query_builder import build_sdmx_key
from ..services import get_services


def validate_obs_counts(*counts: int | None) -> None:
    bad = [c for c in counts if c is not None and c < 1]
    if bad:
        raise NBBValidationError(
            f"last_n_observations / first_n_observations must be >= 1, got {bad}",
            code="INVALID_OBS_COUNT",
        )


async def run(
    dataflow_id: str,
    *,
    agency: str = "BE2",
    version: str | None = None,
    key: str | None = None,
    filters: dict[str, Any] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    first_n_observations: int | None = None,
    max_observations: int = 200,
    max_series: int = DEFAULT_MAX_SERIES,
    format: str = "summary",
) -> dict[str, Any]:
    validate_format(format)
    validate_obs_counts(last_n_observations, first_n_observations)
    if key and filters:
        raise NBBValidationError(
            "Pass either 'key' or 'filters', not both.",
            code="CONFLICTING_ARGS",
        )

    svc = get_services()
    entry = svc.catalog.get(agency, dataflow_id)
    resolved_version = version or entry.version

    if key is None:
        key = build_sdmx_key(entry, filters or {})

    payload = await svc.client.fetch_data(
        agency,
        dataflow_id,
        resolved_version,
        key,
        start_period=start_period,
        end_period=end_period,
        last_n_observations=last_n_observations,
        first_n_observations=first_n_observations,
    )
    dataflow_ref = f"{agency}/{dataflow_id}/{resolved_version}"
    msg = parse_data_message(payload, dataflow=dataflow_ref)
    response = format_data_message(
        msg,
        fmt=format,
        max_observations=max_observations,
        max_series=max_series,
        dataflow_ref=dataflow_ref,
        key=key,
        entry=entry,
    )
    return response.model_dump(exclude_none=True)
