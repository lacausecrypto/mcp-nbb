"""``nbb_compare`` — align 2-5 SDMX series on a common time index.

For each input series the tool fetches the data, picks the first matching
series (to defend against multi-series flows), and merges periods into a wide
table ``{period, <label1>, <label2>, ...}``. Series of different frequencies
are downsampled to the coarsest among the inputs using the closing (last-of-
period) aggregation rule.

Frequency ordering (finest → coarsest): ``D < W < M < Q < S < A``.
"""

from __future__ import annotations

from typing import Any

from ..formatter import _compute_series_stats
from ..models.errors import NBBValidationError
from ..models.sdmx import DataMessage, Observation, Series
from ..parsers.sdmx_json_v2 import parse_data_message
from ..query_builder import build_sdmx_key
from ..services import get_services

MAX_SERIES = 5
MIN_SERIES = 2

FREQ_RANK = {"D": 0, "W": 1, "M": 2, "Q": 3, "S": 4, "A": 5}


def _period_to_coarser(period: str, target_freq: str) -> str | None:
    """Coerce a finer-grained period to the coarser frequency bucket.

    ``2024-03-15`` + target ``M`` → ``2024-03``.
    ``2024-03`` + target ``Q`` → ``2024-Q1``.
    """
    if not period:
        return None
    if target_freq == "A":
        return period[:4] if len(period) >= 4 else None
    if target_freq == "S":
        if len(period) >= 7 and period[4] == "-" and period[5:7].isdigit():
            m = int(period[5:7])
            return f"{period[:4]}-S{1 if m <= 6 else 2}"
        return period[:4] if len(period) >= 4 else None
    if target_freq == "Q":
        if "-Q" in period:
            return period
        if len(period) >= 7 and period[4] == "-" and period[5:7].isdigit():
            m = int(period[5:7])
            q = (m - 1) // 3 + 1
            return f"{period[:4]}-Q{q}"
        return period[:4] if len(period) >= 4 else None
    if target_freq == "M":
        return period[:7] if len(period) >= 7 else period
    if target_freq == "W":
        return period[:10] if len(period) >= 10 else period
    return period


def _downsample(series: Series, target_freq: str) -> dict[str, float | None]:
    """Return one value per coarsened period (closing aggregation)."""
    bucketed: dict[str, Observation] = {}
    for o in sorted(series.observations, key=lambda x: x.period):
        bucket = _period_to_coarser(o.period, target_freq)
        if bucket is None:
            continue
        # Closing rule: the last observation in the bucket wins (for finer → coarser).
        if o.value is not None or bucket not in bucketed:
            bucketed[bucket] = o
    return {p: o.value for p, o in bucketed.items()}


def _pick_target_frequency(detected: list[str | None], requested: str | None) -> str | None:
    if requested:
        return requested
    known = [f for f in detected if f in FREQ_RANK]
    if not known:
        return None
    return max(known, key=lambda f: FREQ_RANK[f])


def _first_series(msg: DataMessage) -> Series | None:
    return msg.series[0] if msg.series else None


async def run(
    series: list[dict[str, Any]],
    *,
    start_period: str | None = None,
    end_period: str | None = None,
    frequency: str | None = None,
) -> dict[str, Any]:
    if not series or len(series) < MIN_SERIES:
        raise NBBValidationError(
            f"nbb_compare requires at least {MIN_SERIES} series",
            code="TOO_FEW_SERIES",
        )
    if len(series) > MAX_SERIES:
        raise NBBValidationError(
            f"nbb_compare accepts at most {MAX_SERIES} series",
            code="TOO_MANY_SERIES",
        )

    svc = get_services()
    fetched: list[tuple[str, Series | None, str | None]] = []
    for i, spec in enumerate(series):
        if "dataflow_id" not in spec:
            raise NBBValidationError(
                f"series[{i}] is missing 'dataflow_id'",
                code="MISSING_DATAFLOW_ID",
            )
        agency = spec.get("agency") or "BE2"
        flow_id = spec["dataflow_id"]
        label = spec.get("label") or f"{agency}/{flow_id}"
        entry = svc.catalog.get(agency, flow_id)
        key = spec.get("key") or build_sdmx_key(entry, spec.get("filters") or {})
        payload = await svc.client.fetch_data(
            agency,
            flow_id,
            entry.version,
            key,
            start_period=start_period,
            end_period=end_period,
        )
        msg = parse_data_message(payload, dataflow=f"{agency}/{flow_id}/{entry.version}")
        s = _first_series(msg)
        fetched.append((label, s, (s.dimensions.get("FREQ") if s else None)))

    # Resolve target frequency (coarsest).
    target = _pick_target_frequency([f for _, _, f in fetched], frequency)

    # Build the wide table.
    per_series: list[tuple[str, dict[str, float | None]]] = []
    all_periods: set[str] = set()
    for label, s, _freq in fetched:
        if s is None:
            per_series.append((label, {}))
            continue
        if target:
            bucketed = _downsample(s, target)
        else:
            bucketed = {o.period: o.value for o in s.observations}
        all_periods.update(bucketed.keys())
        per_series.append((label, bucketed))

    sorted_periods = sorted(all_periods)
    rows: list[dict[str, Any]] = []
    for p in sorted_periods:
        row: dict[str, Any] = {"period": p}
        for label, values in per_series:
            row[label] = values.get(p)
        rows.append(row)

    # Per-series summary stats on the aligned data.
    summaries: list[dict[str, Any]] = []
    for label, s, _freq in fetched:
        if s is None:
            summaries.append({"label": label, "observation_count": 0})
            continue
        stats = _compute_series_stats(s)
        summaries.append(
            {
                "label": label,
                "key": s.key,
                "observation_count": stats.observation_count,
                "period_start": stats.period_start,
                "period_end": stats.period_end,
                "min": stats.min,
                "max": stats.max,
                "mean": stats.mean,
                "latest": stats.latest.model_dump() if stats.latest else None,
                "change_pct": stats.change_pct,
            }
        )

    return {
        "target_frequency": target,
        "period_count": len(sorted_periods),
        "series_labels": [label for label, _ in per_series],
        "summaries": summaries,
        "data": rows,
        "metadata": {
            "start_period": start_period,
            "end_period": end_period,
            "requested_frequency": frequency,
        },
    }
