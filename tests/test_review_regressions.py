"""Regression tests for the bugs fixed in the deps/mcp2 review.

Each test pins one defect that shipped in 0.1.2.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from nbb_mcp.client import NBBClient, _DiskCacheTransport
from nbb_mcp.config import ConfigError, Settings
from nbb_mcp.formatter import format_data_message
from nbb_mcp.models.catalog import EnrichedDataflow
from nbb_mcp.models.errors import NBBValidationError
from nbb_mcp.models.sdmx import (
    Code,
    DataflowDetail,
    DataflowStub,
    DataMessage,
    DataStructure,
    Dimension,
    Observation,
    Series,
)
from nbb_mcp.parsers.sdmx_json_v1 import parse_available_constraint
from nbb_mcp.scripts.build_catalog import _fiche_content_equal, build_fiche
from nbb_mcp.tools.compare import (
    _downsample,
    _pick_target_frequency,
    bucket_label,
    parse_period,
)
from nbb_mcp.tools.quick import TOPICS

# --------------------------------------------------------------------------- #
# Error messages carry code + details (mcp forwards only str(exc)).
# --------------------------------------------------------------------------- #


def test_error_str_includes_code_and_details():
    exc = NBBValidationError(
        "Unknown dimension(s)",
        code="UNKNOWN_DIMENSION",
        details={"valid_dimensions": ["FREQ", "EXR_CURRENCY"]},
    )
    text = str(exc)
    assert text.startswith("[UNKNOWN_DIMENSION] Unknown dimension(s)")
    assert '"valid_dimensions": ["FREQ", "EXR_CURRENCY"]' in text


def test_error_str_without_details_is_single_line():
    assert str(NBBValidationError("boom", code="X")) == "[X] boom"


# --------------------------------------------------------------------------- #
# Formatter token budget.
# --------------------------------------------------------------------------- #


def _many_series(n_series: int, n_obs: int) -> DataMessage:
    return DataMessage(
        dataflow="BE2/X/1.0",
        series=[
            Series(
                key=f"Q.S{i:03d}",
                dimensions={"FREQ": "Q", "ITEM": f"S{i:03d}"},
                observations=[
                    Observation(period=f"{2000 + j // 4}-Q{j % 4 + 1}", value=float(j))
                    for j in range(n_obs)
                ],
            )
            for i in range(n_series)
        ],
    )


def test_series_stats_are_capped_by_max_series():
    r = format_data_message(_many_series(500, 4), max_series=20)
    assert len(r.series) == 20
    assert r.summary["series_count"] == 500
    assert r.summary["series_returned"] == 20
    assert r.truncated is True
    assert r.next_page_hint is not None
    assert "500 series matched" in r.next_page_hint["reason"]
    # Rows only come from the returned series.
    assert {row["series_key"] for row in r.data} <= {s.key for s in r.series}


def test_series_format_respects_max_observations():
    r = format_data_message(_many_series(3, 100), fmt="series", max_observations=10)
    returned = sum(len(s["observations"]) for s in r.data)
    assert returned == 10
    assert r.truncated is True


def test_table_format_omits_per_series_stats():
    r = format_data_message(_many_series(2, 4), fmt="table")
    assert r.series == []
    assert r.data


def test_truncation_hint_names_oldest_returned_period():
    r = format_data_message(_many_series(1, 40), max_observations=8)
    assert r.next_page_hint is not None
    assert r.next_page_hint["retry_with"]["oldest_returned_period"] == r.data[0]["period"]


def test_formatter_payload_size_stays_bounded():
    """The 0.1.2 nbb_quick('gdp') answer was ~62k tokens; keep it well under 25k."""
    r = format_data_message(_many_series(539, 12))
    size = len(json.dumps(r.model_dump(exclude_none=True)))
    assert size < 40_000


# --------------------------------------------------------------------------- #
# nbb_compare period handling.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("period", "target", "expected"),
    [
        ("2024-03-15", "M", "2024-03"),
        ("2024-03-15", "W", "2024-W11"),
        ("2024-03", "Q", "2024-Q1"),
        ("2024-Q3", "S", "2024-S2"),  # used to collapse to "2024"
        ("2024-Q2", "S", "2024-S1"),
        ("2024-W05", "M", "2024-02"),  # Thursday of W05 is 2024-02-01
        ("2025-W01", "A", "2025"),  # ISO week 1 of 2025 starts 2024-12-30
        ("2024-S2", "A", "2024"),
        ("2024", "A", "2024"),
    ],
)
def test_bucket_label(period: str, target: str, expected: str):
    parsed = parse_period(period)
    assert parsed is not None
    assert bucket_label(parsed[1], target) == expected


def test_downsample_closing_rule_keeps_last_non_null():
    s = Series(
        key="D.USD",
        observations=[
            Observation(period="2024-01-30", value=1.0),
            Observation(period="2024-01-31", value=2.0),
            Observation(period="2024-02-01", value=None),
        ],
    )
    assert _downsample(s, "M") == {"2024-01": 2.0, "2024-02": None}


def test_compare_refuses_upsampling():
    with pytest.raises(NBBValidationError) as exc:
        _pick_target_frequency(["Q", "M"], "M")
    assert exc.value.code == "FREQUENCY_TOO_FINE"


def test_compare_picks_coarsest_frequency():
    assert _pick_target_frequency(["D", "Q", "M"], None) == "Q"


# --------------------------------------------------------------------------- #
# Settings read the environment at instantiation and reject garbage.
# --------------------------------------------------------------------------- #


def test_settings_read_env_at_instantiation(monkeypatch):
    monkeypatch.setenv("NBB_API_TIMEOUT", "12.5")
    assert Settings().api_timeout == 12.5


@pytest.mark.parametrize(
    ("var", "value"),
    [
        ("NBB_API_TIMEOUT", "abc"),
        ("NBB_RETRY_ATTEMPTS", "0"),
        ("NBB_HTTP_CACHE_ENABLED", "maybe"),
        ("NBB_LOG_LEVEL", "LOUD"),
    ],
)
def test_settings_reject_invalid_env(monkeypatch, var: str, value: str):
    monkeypatch.setenv(var, value)
    with pytest.raises(ConfigError, match=var):
        Settings()


# --------------------------------------------------------------------------- #
# Available-constraint enrichment.
# --------------------------------------------------------------------------- #

_CONSTRAINT_PAYLOAD = {
    "data": {
        "dataConstraints": [
            {
                "id": "CC",
                "annotations": [{"id": "obs_count", "title": "47211"}],
                "cubeRegions": [
                    {
                        "include": True,
                        "keyValues": [
                            {"id": "FREQ", "values": [{"value": "A"}, {"value": "Q"}]},
                            {"id": "ITEM", "values": [{"value": "B1GM"}]},
                            {
                                "id": "TIME_PERIOD",
                                "timeRange": {
                                    "startPeriod": {"period": "1995-01-01T00:00:00"},
                                    "endPeriod": {"period": "2026-09-30T23:59:59"},
                                },
                            },
                        ],
                    }
                ],
            }
        ]
    }
}


def test_parse_available_constraint():
    c = parse_available_constraint(_CONSTRAINT_PAYLOAD)
    assert c.values == {"FREQ": ["A", "Q"], "ITEM": ["B1GM"]}
    assert (c.start, c.end) == ("1995-01-01", "2026-09-30")
    assert c.observation_count == 47211


def _detail() -> DataflowDetail:
    return DataflowDetail(
        agency="BE2",
        id="DF_X",
        version="1.0",
        structure=DataStructure(
            agency="BE2",
            id="DSD_X",
            version="1.0",
            dimensions=[
                Dimension(
                    id="FREQ", position=0, codes=[Code(id=c) for c in ["A", "S", "Q", "M", "D"]]
                ),
                Dimension(id="ITEM", position=1, codes=[Code(id="B1GM"), Code(id="B11")]),
            ],
        ),
    )


def test_build_fiche_narrows_codes_and_frequencies_to_available_data():
    stub = DataflowStub(agency="BE2", id="DF_X", version="1.0")
    fiche = build_fiche(
        stub,
        _detail(),
        category="other",
        multilang_names={},
        available=parse_available_constraint(_CONSTRAINT_PAYLOAD),
    )
    assert fiche.frequencies_available == ["A", "Q"]
    assert fiche.default_frequency == "Q"
    item = next(d for d in fiche.dimensions if d.id == "ITEM")
    assert [c.id for c in item.codes] == ["B1GM"]
    assert item.total_codes == 1
    assert fiche.time_coverage.end == "2026-09-30"
    assert fiche.availability_filtered is True


def test_build_fiche_without_availability_keeps_full_codelists():
    stub = DataflowStub(agency="BE2", id="DF_X", version="1.0")
    fiche = build_fiche(stub, _detail(), category="other", multilang_names={})
    assert fiche.frequencies_available == ["A", "S", "Q", "M", "D"]
    assert fiche.availability_filtered is False


def _fiche(**overrides) -> EnrichedDataflow:
    base = {
        "agency": "BE2",
        "id": "DF_X",
        "version": "1.0",
        "category": "other",
        "observation_count": 10,
        "time_coverage": {"start": "2000", "end": "2024"},
        "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    base.update(overrides)
    return EnrichedDataflow.model_validate(base)


def test_volatile_coverage_changes_do_not_count_as_drift():
    old = _fiche()
    new = _fiche(observation_count=99, time_coverage={"start": "2000", "end": "2025"})
    assert _fiche_content_equal(old.model_dump_json(), new) is True


def test_stale_fiche_is_refreshed_even_without_drift():
    stale_at = (datetime.now(UTC) - timedelta(days=120)).isoformat(timespec="seconds")
    old = _fiche(fetched_at=stale_at)
    assert _fiche_content_equal(old.model_dump_json(), _fiche()) is False


def test_structural_change_is_drift():
    assert _fiche_content_equal(_fiche().model_dump_json(), _fiche(category="prices")) is False


# --------------------------------------------------------------------------- #
# Client: usage counters and corrupt cache entries.
# --------------------------------------------------------------------------- #


def _ok_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        headers={"content-type": "application/vnd.sdmx.structure+json"},
        content=b'{"data": {}}',
    )


async def test_client_counts_requests_and_errors(test_settings: Settings):
    def handler(request: httpx.Request) -> httpx.Response:
        if "missing" in request.url.path:
            return httpx.Response(404, text="NoRecordsFound")
        return _ok_handler(request)

    client = NBBClient(settings=test_settings, transport=httpx.MockTransport(handler))
    try:
        await client.get_structure("dataflow/all/all/latest")
        with pytest.raises(Exception, match="NOT_FOUND"):
            await client.get_structure("dataflow/BE2/missing/1.0")
    finally:
        await client.aclose()
    assert client.stats.request_count == 2
    assert client.stats.error_count == 1
    assert client.stats.last_error is not None and "NOT_FOUND" in client.stats.last_error


async def test_corrupt_cache_entry_is_a_miss(tmp_path: Path):
    transport = _DiskCacheTransport(
        httpx.MockTransport(_ok_handler), cache_dir=tmp_path, structure_ttl=3600, data_ttl=60
    )
    request = httpx.Request("GET", "https://example.test/rest/dataflow/BE2/DF_X/1.0")
    key = transport._key(request)
    key.write_text(json.dumps({"stored_at": 9e18, "body_b64": "!!not base64!!", "headers": []}))
    response = await transport.handle_async_request(request)
    assert response.status_code == 200
    assert response.extensions["from_cache"] is False


# --------------------------------------------------------------------------- #
# nbb_quick headline routing.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("topic_name", sorted(TOPICS))
def test_every_quick_topic_resolves_against_bundled_catalog(topic_name: str):
    """Topic codes must exist (and hold data) in the shipped catalogue."""
    from nbb_mcp.catalog import Catalog
    from nbb_mcp.query_builder import build_sdmx_key
    from nbb_mcp.tools.quick import topic_filters

    topic = TOPICS[topic_name]
    entry = Catalog.load().get(topic.agency, topic.dataflow_id)
    key = build_sdmx_key(entry, topic_filters(topic))
    assert key != "all"
    if topic.frequency:
        assert topic.frequency in entry.frequencies_available


def test_quick_currency_rejected_outside_exchange_rate():
    from nbb_mcp.tools.quick import topic_filters

    with pytest.raises(NBBValidationError) as exc:
        topic_filters(TOPICS["gdp"], currency="USD")
    assert exc.value.code == "UNSUPPORTED_ARG"


def test_quick_frequency_applies_to_every_topic():
    from nbb_mcp.tools.quick import topic_filters

    assert topic_filters(TOPICS["gdp"], frequency="a")["FREQ"] == "A"
    assert topic_filters(TOPICS["exchange_rate"], currency="gbp")["EXR_CURRENCY"] == "GBP"


def test_quick_unavailable_frequency_lists_valid_codes():
    from nbb_mcp.catalog import Catalog
    from nbb_mcp.query_builder import build_sdmx_key
    from nbb_mcp.tools.quick import topic_filters

    entry = Catalog.load().get("BE2", "DF_QNA_DISS")
    with pytest.raises(NBBValidationError) as exc:
        build_sdmx_key(entry, topic_filters(TOPICS["gdp"], frequency="D"))
    assert exc.value.code == "UNKNOWN_CODE"
    assert "Q" in exc.value.details["valid_codes_sample"]


def test_empty_result_emits_actionable_hint():
    r = format_data_message(DataMessage(dataflow="x", series=[]))
    assert r.next_page_hint is not None
    assert "No series matched" in r.next_page_hint["reason"]


def test_package_version_matches_pyproject():
    import tomllib

    import nbb_mcp

    pyproject = tomllib.loads((Path(__file__).parent.parent / "pyproject.toml").read_text())
    assert nbb_mcp.__version__ == pyproject["project"]["version"]
