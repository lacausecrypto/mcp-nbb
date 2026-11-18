"""Convert parsed SDMX :class:`DataMessage` instances into LLM-friendly payloads.

Responsibilities:
- Compute aggregate stats (count, period range) and per-series stats.
- Flatten observations into compact rows with optional series_key tagging.
- Enforce a token budget via ``max_observations`` (rows) and ``max_series``
  (per-series stats) and emit a ``next_page_hint`` when either cap bites.
- Support three output modes:
    * ``summary`` — summary + series stats + data rows (default).
    * ``table`` — summary + data rows, without per-series stats (most compact).
    * ``series`` — per-series grouping (no flattening) for debug.
"""

from __future__ import annotations

from typing import Any

from .models.catalog import EnrichedDataflow
from .models.errors import NBBValidationError
from .models.responses import FormattedResponse, ObservationPoint, SeriesStats
from .models.sdmx import DataMessage, Series

VALID_FORMATS = ("summary", "table", "series")
DEFAULT_MAX_OBSERVATIONS = 200
DEFAULT_MAX_SERIES = 20


def _compute_series_stats(series: Series) -> SeriesStats:
    obs_all = series.observations
    if not obs_all:
        return SeriesStats(
            key=series.key,
            dimensions=dict(series.dimensions),
            observation_count=0,
        )

    sorted_obs = sorted(obs_all, key=lambda o: o.period)
    valued = [o for o in sorted_obs if o.value is not None]
    period_start = sorted_obs[0].period
    period_end = sorted_obs[-1].period

    if not valued:
        return SeriesStats(
            key=series.key,
            dimensions=dict(series.dimensions),
            observation_count=len(sorted_obs),
            period_start=period_start,
            period_end=period_end,
        )

    values = [o.value for o in valued if o.value is not None]
    first = valued[0]
    last = valued[-1]
    min_v = min(values)
    max_v = max(values)
    mean_v = sum(values) / len(values)

    change_abs: float | None = None
    change_pct: float | None = None
    if first.value is not None and last.value is not None:
        change_abs = last.value - first.value
        if first.value != 0:
            change_pct = change_abs / first.value * 100

    return SeriesStats(
        key=series.key,
        dimensions=dict(series.dimensions),
        observation_count=len(sorted_obs),
        period_start=period_start,
        period_end=period_end,
        first=ObservationPoint(period=first.period, value=first.value),
        latest=ObservationPoint(period=last.period, value=last.value),
        min=min_v,
        max=max_v,
        mean=mean_v,
        change_absolute=change_abs,
        change_pct=change_pct,
    )


def validate_format(fmt: str) -> None:
    if fmt not in VALID_FORMATS:
        raise NBBValidationError(
            f"Unknown format '{fmt}'",
            code="INVALID_FORMAT",
            details={"valid_formats": list(VALID_FORMATS)},
        )


def format_data_message(
    msg: DataMessage,
    *,
    fmt: str = "summary",
    max_observations: int = DEFAULT_MAX_OBSERVATIONS,
    max_series: int = DEFAULT_MAX_SERIES,
    dataflow_ref: str | None = None,
    key: str | None = None,
    entry: EnrichedDataflow | None = None,
) -> FormattedResponse:
    """Shape a parsed message for an LLM, within a token budget.

    Both budgets are hard caps on the payload: ``max_series`` bounds the
    per-series stats (and the series whose rows are returned), and
    ``max_observations`` bounds the returned rows in every format. The summary
    always reflects the full upstream response.
    """
    validate_format(fmt)
    max_observations = max(1, int(max_observations))
    max_series = max(1, int(max_series))

    all_stats = [_compute_series_stats(s) for s in msg.series]
    total_obs = sum(s.observation_count for s in all_stats)
    summary: dict[str, Any] = {
        "series_count": len(all_stats),
        "total_observations": total_obs,
    }
    period_starts = [s.period_start for s in all_stats if s.period_start]
    period_ends = [s.period_end for s in all_stats if s.period_end]
    if period_starts:
        summary["period_start"] = min(period_starts)
    if period_ends:
        summary["period_end"] = max(period_ends)

    shown = msg.series[:max_series]
    series_stats = all_stats[:max_series]
    series_truncated = len(msg.series) > max_series
    if series_truncated:
        summary["series_returned"] = len(shown)

    multi_series = len(shown) > 1
    # (period, series index, value) — the index lets the "series" format regroup.
    flat = [(o.period, i, o.value) for i, s in enumerate(shown) for o in s.observations]
    flat.sort(key=lambda t: (t[0], shown[t[1]].key))

    obs_truncated = len(flat) > max_observations
    if obs_truncated:
        flat = flat[-max_observations:]

    data: list[dict[str, Any]]
    if fmt == "series":
        grouped: dict[int, list[dict[str, Any]]] = {}
        for period, i, value in flat:
            grouped.setdefault(i, []).append({"period": period, "value": value})
        data = [
            {
                "key": shown[i].key,
                "dimensions": dict(shown[i].dimensions),
                "observations": grouped.get(i, []),
            }
            for i in range(len(shown))
        ]
    else:
        data = []
        for period, i, value in flat:
            row: dict[str, Any] = {"period": period, "value": value}
            if multi_series:
                row["series_key"] = shown[i].key
            data.append(row)

    next_hint: dict[str, Any] | None = None
    if not msg.series:
        empty_hint: dict[str, Any] = {
            "check": "codes and time_coverage in nbb_describe; widen the period window",
        }
        if entry is not None:
            empty_hint["time_coverage"] = entry.time_coverage.model_dump()
            if entry.key_template:
                empty_hint["key_template"] = entry.key_template
        next_hint = {"reason": "No series matched this key and period.", "retry_with": empty_hint}
    elif obs_truncated or series_truncated:
        reasons: list[str] = []
        hint: dict[str, Any] = {}
        if series_truncated:
            reasons.append(
                f"{len(msg.series)} series matched; stats and rows cover the first "
                f"{len(shown)} only."
            )
            hint["narrow_with"] = (
                "filters/key on more dimensions (see nbb_describe for codes that hold data)"
            )
            if entry is not None and entry.key_template:
                hint["key_template"] = entry.key_template
        if obs_truncated:
            oldest = flat[0][0] if flat else None
            reasons.append(f"Rows truncated to the latest {max_observations} observations.")
            hint["oldest_returned_period"] = oldest
            hint["older_data"] = "pass end_period earlier than oldest_returned_period"
        next_hint = {"reason": " ".join(reasons), "retry_with": hint}

    metadata: dict[str, Any] = {
        "dataflow": dataflow_ref,
        "key": key,
        "format": fmt,
    }
    if entry is not None:
        metadata["primary_measure"] = entry.primary_measure
        if entry.default_frequency:
            metadata["default_frequency"] = entry.default_frequency

    return FormattedResponse(
        summary=summary,
        series=[] if fmt == "table" else series_stats,
        data=data,
        metadata=metadata,
        truncated=obs_truncated or series_truncated,
        next_page_hint=next_hint,
    )


__all__ = [
    "DEFAULT_MAX_OBSERVATIONS",
    "DEFAULT_MAX_SERIES",
    "VALID_FORMATS",
    "format_data_message",
    "validate_format",
]
