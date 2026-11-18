"""``nbb_compare`` — align 2-5 SDMX series on a common time index.

Each input spec must resolve to exactly one series (an ambiguous key is an
error, never a silent pick). Periods are merged into a wide table
``{period, <label1>, <label2>, ...}``. Series of different frequencies are
downsampled to the coarsest among the inputs (or to ``frequency`` if given)
using the closing (last-of-period) aggregation rule. Upsampling is refused.

Frequency ordering (finest → coarsest): ``D < W < M < Q < S < A``.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from ..formatter import _compute_series_stats
from ..models.errors import NBBValidationError
from ..models.sdmx import Series
from ..parsers.sdmx_json_v2 import parse_data_message
from ..query_builder import build_sdmx_key
from ..services import get_services

MAX_SERIES = 5
MIN_SERIES = 2
DEFAULT_MAX_PERIODS = 200

FREQ_RANK = {"D": 0, "B": 0, "W": 1, "M": 2, "Q": 3, "S": 4, "A": 5}

_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("D", re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")),
    ("W", re.compile(r"^(\d{4})-W(\d{2})$")),
    ("M", re.compile(r"^(\d{4})-M?(\d{2})$")),
    ("Q", re.compile(r"^(\d{4})-Q([1-4])$")),
    ("S", re.compile(r"^(\d{4})-[SH]([12])$")),
    ("A", re.compile(r"^(\d{4})(?:-A1)?$")),
)


def parse_period(period: str) -> tuple[str, date] | None:
    """Return ``(frequency, anchor_date)`` for an SDMX period string.

    The anchor is the date used to place the period in a coarser bucket: the
    first day for D/M/Q/S/A, and the Thursday for ISO weeks (the ISO rule that
    decides which month/year a week belongs to).
    """
    for freq, pattern in _PATTERNS:
        m = pattern.match(period)
        if not m:
            continue
        year = int(m.group(1))
        try:
            if freq == "D":
                return freq, date(year, int(m.group(2)), int(m.group(3)))
            if freq == "W":
                return freq, date.fromisocalendar(year, int(m.group(2)), 4)
            if freq == "M":
                return freq, date(year, int(m.group(2)), 1)
            if freq == "Q":
                return freq, date(year, (int(m.group(2)) - 1) * 3 + 1, 1)
            if freq == "S":
                return freq, date(year, (int(m.group(2)) - 1) * 6 + 1, 1)
            return freq, date(year, 1, 1)
        except ValueError:
            return None
    return None


def bucket_label(anchor: date, target_freq: str) -> str:
    if target_freq == "A":
        return f"{anchor.year}"
    if target_freq == "S":
        return f"{anchor.year}-S{1 if anchor.month <= 6 else 2}"
    if target_freq == "Q":
        return f"{anchor.year}-Q{(anchor.month - 1) // 3 + 1}"
    if target_freq == "M":
        return f"{anchor.year}-{anchor.month:02d}"
    if target_freq == "W":
        # A daily anchor falls in the ISO week containing it.
        iso = anchor.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return anchor.isoformat()


def detect_frequency(series: Series) -> str | None:
    freq = series.dimensions.get("FREQ")
    if freq in FREQ_RANK:
        return freq
    for o in series.observations:
        parsed = parse_period(o.period)
        if parsed:
            return parsed[0]
    return None


def _downsample(series: Series, target_freq: str) -> dict[str, float | None]:
    """Return one value per target period (closing aggregation)."""
    bucketed: dict[str, float | None] = {}
    for o in sorted(series.observations, key=lambda x: x.period):
        parsed = parse_period(o.period)
        if parsed is None:
            raise NBBValidationError(
                f"Cannot interpret period '{o.period}' of series {series.key}",
                code="UNPARSEABLE_PERIOD",
                details={"series_key": series.key, "period": o.period},
            )
        bucket = bucket_label(parsed[1], target_freq)
        # Closing rule: the last non-null observation in the bucket wins.
        if o.value is not None or bucket not in bucketed:
            bucketed[bucket] = o.value
    return bucketed


def _pick_target_frequency(detected: list[str | None], requested: str | None) -> str | None:
    known = [f for f in detected if f in FREQ_RANK]
    if requested:
        coarser = [f for f in known if FREQ_RANK[f] > FREQ_RANK[requested]]
        if coarser:
            raise NBBValidationError(
                f"Cannot upsample to '{requested}': input frequencies {sorted(set(coarser))} "
                "are coarser",
                code="FREQUENCY_TOO_FINE",
                details={"requested": requested, "detected": known},
            )
        return requested
    if not known:
        return None
    return max(known, key=lambda f: FREQ_RANK[f])


async def run(
    series: list[dict[str, Any]],
    *,
    start_period: str | None = None,
    end_period: str | None = None,
    frequency: str | None = None,
    max_periods: int = DEFAULT_MAX_PERIODS,
) -> dict[str, Any]:
    max_periods = max(1, int(max_periods))
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
    if frequency is not None:
        frequency = frequency.strip().upper()
        if frequency not in FREQ_RANK:
            raise NBBValidationError(
                f"Unknown frequency '{frequency}'",
                code="INVALID_FREQUENCY",
                details={"valid_frequencies": sorted(FREQ_RANK)},
            )

    svc = get_services()
    labels: list[str] = []
    fetched: list[tuple[str, Series | None]] = []
    for i, spec in enumerate(series):
        if "dataflow_id" not in spec:
            raise NBBValidationError(
                f"series[{i}] is missing 'dataflow_id'",
                code="MISSING_DATAFLOW_ID",
            )
        if spec.get("key") and spec.get("filters"):
            raise NBBValidationError(
                f"series[{i}]: pass either 'key' or 'filters', not both",
                code="CONFLICTING_ARGS",
            )
        agency = spec.get("agency") or "BE2"
        flow_id = spec["dataflow_id"]
        label = spec.get("label") or f"{agency}/{flow_id}"
        if label in labels:
            label = f"{label} #{i + 1}"
        labels.append(label)
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
        if len(msg.series) > 1:
            raise NBBValidationError(
                f"series[{i}] ({agency}/{flow_id}, key '{key}') matches "
                f"{len(msg.series)} series; nbb_compare needs exactly one per spec",
                code="AMBIGUOUS_SERIES",
                details={
                    "matched_keys_sample": [s.key for s in msg.series[:10]],
                    "key_template": entry.key_template,
                },
            )
        fetched.append((label, msg.series[0] if msg.series else None))

    target = _pick_target_frequency(
        [detect_frequency(s) if s else None for _, s in fetched], frequency
    )

    per_series: list[tuple[str, dict[str, float | None]]] = []
    all_periods: set[str] = set()
    for label, s in fetched:
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
    total_periods = len(sorted_periods)
    truncated = total_periods > max_periods
    if truncated:
        sorted_periods = sorted_periods[-max_periods:]
    rows: list[dict[str, Any]] = []
    for p in sorted_periods:
        row: dict[str, Any] = {"period": p}
        for label, values in per_series:
            row[label] = values.get(p)
        rows.append(row)

    # Per-series stats on the raw (pre-alignment) observations.
    summaries: list[dict[str, Any]] = []
    for label, s in fetched:
        if s is None:
            summaries.append({"label": label, "observation_count": 0})
            continue
        stats = _compute_series_stats(s)
        summaries.append(
            {
                "label": label,
                "key": s.key,
                "frequency": detect_frequency(s),
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
        "period_count": total_periods,
        "truncated": truncated,
        **(
            {
                "next_page_hint": {
                    "reason": f"Aligned table truncated to the latest {max_periods} periods.",
                    "oldest_returned_period": sorted_periods[0],
                    "older_data": "pass end_period earlier than oldest_returned_period, "
                    "or a coarser 'frequency'",
                }
            }
            if truncated
            else {}
        ),
        "series_labels": [label for label, _ in per_series],
        "summaries": summaries,
        "data": rows,
        "metadata": {
            "start_period": start_period,
            "end_period": end_period,
            "requested_frequency": frequency,
        },
    }
