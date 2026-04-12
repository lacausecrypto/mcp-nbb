"""Tests for SDMX-JSON parsers using real NBB fixtures."""

from __future__ import annotations

import pytest

from nbb_mcp.models.errors import NBBParseError
from nbb_mcp.parsers.sdmx_json_v1 import parse_dataflow_detail, parse_dataflow_list
from nbb_mcp.parsers.sdmx_json_v2 import parse_data_message

# ---------------------------------------------------------------------------
# Structure parser (SDMX-JSON 1.0)
# ---------------------------------------------------------------------------


def test_parse_dataflow_list_counts_and_agencies(dataflow_list_payload):
    stubs = parse_dataflow_list(dataflow_list_payload)
    assert len(stubs) == 221
    agencies = {s.agency for s in stubs}
    assert agencies == {"BE2", "IMF"}
    df_exr = next(s for s in stubs if s.id == "DF_EXR" and s.agency == "BE2")
    assert df_exr.version == "1.0"
    assert df_exr.names.get("en", "").lower().startswith("reference exchange")


def test_parse_dataflow_list_invalid_root_raises():
    with pytest.raises(NBBParseError):
        parse_dataflow_list("not a dict")  # type: ignore[arg-type]


def test_parse_dataflow_detail_extracts_dimensions(dataflow_detail_payload):
    detail = parse_dataflow_detail(dataflow_detail_payload)
    assert detail.agency == "BE2"
    assert detail.id == "DF_EXR"
    assert detail.structure is not None

    dims = detail.structure.dimensions
    assert len(dims) >= 2
    dim_ids = [d.id for d in dims]
    assert "FREQ" in dim_ids
    assert "EXR_CURRENCY" in dim_ids
    # Dimensions must be ordered by position.
    assert [d.position for d in dims] == sorted([d.position for d in dims])

    freq_dim = next(d for d in dims if d.id == "FREQ")
    assert len(freq_dim.codes) > 0
    freq_code_ids = {c.id for c in freq_dim.codes}
    assert "D" in freq_code_ids  # Daily

    currency_dim = next(d for d in dims if d.id == "EXR_CURRENCY")
    currency_ids = {c.id for c in currency_dim.codes}
    assert "USD" in currency_ids


def test_parse_dataflow_detail_time_dimension(dataflow_detail_payload):
    detail = parse_dataflow_detail(dataflow_detail_payload)
    assert detail.structure is not None
    assert detail.structure.time_dimension == "TIME_PERIOD"


def test_parse_dataflow_detail_empty_dataflows_raises():
    with pytest.raises(NBBParseError):
        parse_dataflow_detail({"data": {"dataflows": []}, "meta": {}})


# ---------------------------------------------------------------------------
# Data parser (SDMX-JSON 2.0)
# ---------------------------------------------------------------------------


def test_parse_data_message_basic(data_payload):
    msg = parse_data_message(data_payload, dataflow="BE2/DF_EXR/1.0")
    assert msg.dataflow == "BE2/DF_EXR/1.0"
    assert len(msg.series) > 0
    assert msg.total_observations > 0


def test_parse_data_message_series_keys_resolved(data_payload):
    msg = parse_data_message(data_payload)
    # Series keys must be resolved to ``<code>.<code>`` (not positional ``0:1``).
    for series in msg.series:
        assert ":" not in series.key
        assert series.key.count(".") >= 1
        assert all(part for part in series.key.split("."))


def test_parse_data_message_dimensions_populated(data_payload):
    msg = parse_data_message(data_payload)
    s = msg.series[0]
    assert "FREQ" in s.dimensions
    assert "EXR_CURRENCY" in s.dimensions
    assert s.dimensions["FREQ"]


def test_parse_data_message_observations_ordered(data_payload):
    msg = parse_data_message(data_payload)
    for series in msg.series:
        periods = [o.period for o in series.observations]
        assert periods == sorted(periods)
        for o in series.observations:
            if o.value is not None:
                assert isinstance(o.value, float)


def test_parse_data_message_has_usd_series(data_payload):
    msg = parse_data_message(data_payload)
    usd = [s for s in msg.series if s.dimensions.get("EXR_CURRENCY") == "USD"]
    assert usd, "Expected at least one USD series in DF_EXR sample"


def test_parse_data_message_invalid_root():
    with pytest.raises(NBBParseError):
        parse_data_message("bad")  # type: ignore[arg-type]
