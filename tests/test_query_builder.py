"""Tests for ``nbb_mcp.query_builder.build_sdmx_key``."""

from __future__ import annotations

import pytest

from nbb_mcp.models.catalog import (
    CatalogDimension,
    CodeRef,
    EnrichedDataflow,
    TimeCoverage,
)
from nbb_mcp.models.errors import NBBValidationError
from nbb_mcp.query_builder import build_sdmx_key


def _entry(dims: list[CatalogDimension]) -> EnrichedDataflow:
    return EnrichedDataflow(
        agency="BE2",
        id="DF_TEST",
        version="1.0",
        category="other",
        is_final=True,
        names={"en": "Test"},
        dimensions=dims,
        primary_measure="OBS_VALUE",
        time_coverage=TimeCoverage(),
    )


def _exr_entry() -> EnrichedDataflow:
    return _entry(
        [
            CatalogDimension(
                id="FREQ",
                position=0,
                codes=[CodeRef(id="D"), CodeRef(id="M"), CodeRef(id="Q")],
                total_codes=3,
            ),
            CatalogDimension(
                id="EXR_CURRENCY",
                position=1,
                codes=[CodeRef(id="USD"), CodeRef(id="GBP"), CodeRef(id="JPY")],
                total_codes=3,
            ),
        ]
    )


def test_empty_filters_returns_all():
    assert build_sdmx_key(_exr_entry(), {}) == "all"
    assert build_sdmx_key(_exr_entry(), None) == "all"


def test_single_dimension_filter():
    assert build_sdmx_key(_exr_entry(), {"FREQ": "D"}) == "D."


def test_both_dimensions_filter():
    assert build_sdmx_key(_exr_entry(), {"FREQ": "D", "EXR_CURRENCY": "USD"}) == "D.USD"


def test_trailing_wildcard_is_preserved():
    assert build_sdmx_key(_exr_entry(), {"FREQ": "D"}) == "D."


def test_leading_wildcard_is_preserved():
    assert build_sdmx_key(_exr_entry(), {"EXR_CURRENCY": "USD"}) == ".USD"


def test_multi_value_filter_uses_plus():
    key = build_sdmx_key(_exr_entry(), {"FREQ": ["D", "M"], "EXR_CURRENCY": "USD"})
    assert key == "D+M.USD"


def test_position_order_respected_regardless_of_input_order():
    key = build_sdmx_key(
        _exr_entry(), {"EXR_CURRENCY": "USD", "FREQ": "D"}
    )
    assert key == "D.USD"


def test_unknown_dimension_raises_with_hints():
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(_exr_entry(), {"BAD_DIM": "X"})
    assert exc.value.code == "UNKNOWN_DIMENSION"
    assert "BAD_DIM" in exc.value.details["unknown_dimensions"]
    assert exc.value.details["valid_dimensions"] == ["FREQ", "EXR_CURRENCY"]


def test_unknown_code_raises_with_hints():
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(_exr_entry(), {"FREQ": "ZZZ"})
    assert exc.value.code == "UNKNOWN_CODE"
    assert exc.value.details["dimension"] == "FREQ"
    assert "ZZZ" in exc.value.details["invalid_codes"]
    assert set(exc.value.details["valid_codes_sample"]) == {"D", "M", "Q"}


def test_multi_value_partial_invalid_raises():
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(_exr_entry(), {"EXR_CURRENCY": ["USD", "XXX"]})
    assert "XXX" in exc.value.details["invalid_codes"]


def test_no_dimensions_in_entry_rejects_filters():
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(_entry([]), {"FREQ": "D"})
    assert exc.value.code == "NO_DIMENSIONS"


def test_dimension_without_codelist_accepts_any_value():
    dims = [
        CatalogDimension(id="FREE", position=0, codes=[], total_codes=0),
    ]
    entry = _entry(dims)
    assert build_sdmx_key(entry, {"FREE": "anything"}) == "anything"


def test_truncated_codelist_validates_against_available_codes_only():
    dims = [
        CatalogDimension(
            id="BIG",
            position=0,
            codes=[CodeRef(id="ONE"), CodeRef(id="TWO")],
            total_codes=1000,
            truncated=True,
        )
    ]
    entry = _entry(dims)
    assert build_sdmx_key(entry, {"BIG": "ONE"}) == "ONE"
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(entry, {"BIG": "NOT_IN_SAMPLE"})
    assert exc.value.details["codelist_truncated_in_catalog"] is True
