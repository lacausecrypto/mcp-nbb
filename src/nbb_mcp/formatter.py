"""Convert parsed SDMX :class:`DataMessage` instances into LLM-friendly payloads.

Responsibilities:
- Compute aggregate stats (count, period range) and per-series stats.
- Flatten observations into compact rows with optional series_key tagging.
- Enforce a token budget via ``max_observations`` and emit a ``next_page_hint``
  when data is truncated.
- Support three output modes:
    * ``summary`` — summary + series stats + data rows (default).
    * ``table`` — same as summary but ``data`` is a compact rows-only list.
    * ``series`` — per-series grouping (no flattening) for debug.
"""

from __future__ import annotations

from typing import Any

from .models.catalog import EnrichedDataflow
from .models.responses import FormattedResponse, ObservationPoint, SeriesStats
from .models.sdmx import DataMessage, Series

VALID_FORMATS = ("summary", "table", "series")
DEFAULT_MAX_OBSERVATIONS = 200


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

    values = [o.value for o in valued]  # type: ignore[misc]
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


def format_data_message(
    msg: DataMessage,
    *,
    fmt: str = "summary",
    max_observations: int = DEFAULT_MAX_OBSERVATIONS,
    dataflow_ref: str | None = None,
    key: str | None = None,
    entry: EnrichedDataflow | None = None,
) -> FormattedResponse:
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Unknown format '{fmt}' — expected one of {VALID_FORMATS}")
    max_observations = max(1, int(max_observations))

    series_stats = [_compute_series_stats(s) for s in msg.series]

    total_obs = sum(s.observation_count for s in series_stats)
    summary: dict[str, Any] = {
        "series_count": len(series_stats),
        "total_observations": total_obs,
    }
    period_starts = [s.period_start for s in series_stats if s.period_start]
    period_ends = [s.period_end for s in series_stats if s.period_end]
    if period_starts:
        summary["period_start"] = min(period_starts)
    if period_ends:
        summary["period_end"] = max(period_ends)

    rows: list[dict[str, Any]] = []
    multi_series = len(msg.series) > 1
    for s in msg.series:
        for obs in s.observations:
            row: dict[str, Any] = {"period": obs.period, "value": obs.value}
            if multi_series:
                row["series_key"] = s.key
            rows.append(row)
    rows.sort(key=lambda r: (r["period"], r.get("series_key", "")))

    truncated = False
    next_hint: dict[str, Any] | None = None
    if len(rows) > max_observations:
        truncated = True
        rows = rows[-max_observations:]
        next_hint = {
            "reason": f"Data truncated to the last {max_observations} observations.",
            "retry_with": {
                "max_observations": max_observations,
                "start_period": "<earlier period>",
                "end_period": "<cutoff before current window>",
            },
        }

    data: list[dict[str, Any]]
    if fmt == "series":
        data = [
            {
                "key": s.key,
                "dimensions": s.dimensions,
                "observations": [
                    {"period": o.period, "value": o.value}
                    for o in sorted(msg.series[i].observations, key=lambda o: o.period)
                ],
            }
            for i, s in enumerate(series_stats)
        ]
    else:
        data = rows

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
        series=series_stats,
        data=data,
        metadata=metadata,
        truncated=truncated,
        next_page_hint=next_hint,
    )


__all__ = ["DEFAULT_MAX_OBSERVATIONS", "VALID_FORMATS", "format_data_message"]
