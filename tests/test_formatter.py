"""Tests for ``nbb_mcp.formatter.format_data_message``."""

from __future__ import annotations

import pytest

from nbb_mcp.formatter import format_data_message
from nbb_mcp.models.sdmx import DataMessage, Observation, Series


def _series(key: str, dims: dict[str, str], values: list[tuple[str, float | None]]) -> Series:
    return Series(
        key=key,
        dimensions=dims,
        observations=[Observation(period=p, value=v) for p, v in values],
    )


def _msg(series: list[Series]) -> DataMessage:
    return DataMessage(dataflow="BE2/DF_EXR/1.0", series=series)


def test_single_series_summary_has_expected_stats():
    msg = _msg(
        [
            _series(
                "D.USD",
                {"FREQ": "D", "EXR_CURRENCY": "USD"},
                [("2024-01-01", 1.10), ("2024-01-02", 1.12), ("2024-01-03", 1.08)],
            )
        ]
    )
    r = format_data_message(msg)
    assert r.summary["series_count"] == 1
    assert r.summary["total_observations"] == 3
    assert r.summary["period_start"] == "2024-01-01"
    assert r.summary["period_end"] == "2024-01-03"
    s = r.series[0]
    assert s.first.value == 1.10
    assert s.latest.value == 1.08
    assert s.min == 1.08
    assert s.max == 1.12
    assert s.mean == pytest.approx(1.10)
    assert s.change_absolute == pytest.approx(-0.02)
    assert s.change_pct == pytest.approx(-1.818, abs=1e-2)


def test_data_rows_sorted_and_single_series_has_no_series_key():
    msg = _msg([_series("D.USD", {"FREQ": "D"}, [("2024-01-03", 1.0), ("2024-01-01", 2.0)])])
    r = format_data_message(msg)
    assert [row["period"] for row in r.data] == ["2024-01-01", "2024-01-03"]
    assert "series_key" not in r.data[0]


def test_multi_series_data_rows_include_series_key():
    msg = _msg(
        [
            _series("D.USD", {"FREQ": "D", "EXR_CURRENCY": "USD"}, [("2024-01-01", 1.10)]),
            _series("D.GBP", {"FREQ": "D", "EXR_CURRENCY": "GBP"}, [("2024-01-01", 0.85)]),
        ]
    )
    r = format_data_message(msg)
    assert r.summary["series_count"] == 2
    assert all("series_key" in row for row in r.data)
    keys = {row["series_key"] for row in r.data}
    assert keys == {"D.USD", "D.GBP"}


def test_token_budget_truncates_and_emits_hint():
    vals = [(f"2024-{m:02d}-01", float(m)) for m in range(1, 13)]
    msg = _msg([_series("M.X", {"FREQ": "M"}, vals)])
    r = format_data_message(msg, max_observations=5)
    assert r.truncated is True
    assert len(r.data) == 5
    assert r.data[-1]["period"] == "2024-12-01"
    assert r.next_page_hint is not None
    assert r.next_page_hint["retry_with"]["max_observations"] == 5


def test_missing_values_handled():
    msg = _msg(
        [
            _series(
                "D.X",
                {},
                [("2024-01-01", None), ("2024-01-02", 1.0), ("2024-01-03", None)],
            )
        ]
    )
    r = format_data_message(msg)
    s = r.series[0]
    assert s.observation_count == 3
    assert s.first.value == 1.0
    assert s.latest.value == 1.0
    assert s.min == 1.0
    assert s.max == 1.0
    assert s.change_absolute == 0.0


def test_empty_series_returns_no_stats():
    msg = _msg([_series("D.X", {}, [])])
    r = format_data_message(msg)
    assert r.summary["total_observations"] == 0
    s = r.series[0]
    assert s.observation_count == 0
    assert s.first is None and s.latest is None
    assert s.min is None


def test_series_format_groups_observations_per_series():
    msg = _msg(
        [
            _series("D.USD", {"FREQ": "D", "EXR_CURRENCY": "USD"}, [("2024-01-01", 1.0)]),
            _series("D.GBP", {"FREQ": "D", "EXR_CURRENCY": "GBP"}, [("2024-01-01", 0.8)]),
        ]
    )
    r = format_data_message(msg, fmt="series")
    assert isinstance(r.data, list)
    assert len(r.data) == 2
    assert {group["key"] for group in r.data} == {"D.USD", "D.GBP"}
    assert all("observations" in group for group in r.data)


def test_invalid_format_raises():
    with pytest.raises(ValueError):
        format_data_message(_msg([]), fmt="banana")


def test_metadata_includes_dataflow_and_key():
    msg = _msg([_series("D.USD", {}, [("2024-01-01", 1.0)])])
    r = format_data_message(msg, dataflow_ref="BE2/DF_EXR/1.0", key="D.USD")
    assert r.metadata["dataflow"] == "BE2/DF_EXR/1.0"
    assert r.metadata["key"] == "D.USD"
    assert r.metadata["format"] == "summary"


def test_zero_first_value_change_pct_is_none():
    msg = _msg([_series("D.X", {}, [("2024-01-01", 0.0), ("2024-01-02", 5.0)])])
    r = format_data_message(msg)
    assert r.series[0].change_absolute == 5.0
    assert r.series[0].change_pct is None
