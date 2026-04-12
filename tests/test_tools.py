"""Integration-ish tests for the 6 MCP tools.

Each test injects a fake :class:`Services` with:
- a small in-memory catalogue built from hand-crafted fiches
- an :class:`NBBClient` wrapping :class:`httpx.MockTransport` to serve canned
  SDMX-JSON responses.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from nbb_mcp.catalog import Catalog
from nbb_mcp.client import NBBClient
from nbb_mcp.config import Settings
from nbb_mcp.models.catalog import (
    CatalogDimension,
    CodeRef,
    CommonQuery,
    EnrichedDataflow,
    TimeCoverage,
)
from nbb_mcp.models.errors import NBBCatalogError, NBBValidationError
from nbb_mcp.services import Services, reset_services
from nbb_mcp.tools import compare as compare_tool
from nbb_mcp.tools import describe as describe_tool
from nbb_mcp.tools import query as query_tool
from nbb_mcp.tools import quick as quick_tool
from nbb_mcp.tools import search as search_tool
from nbb_mcp.tools import status as status_tool

FIXTURES = Path(__file__).parent / "cassettes"


def _fake_exr_entry() -> EnrichedDataflow:
    return EnrichedDataflow(
        agency="BE2",
        id="DF_EXR",
        version="1.0",
        category="exchange_interest_rates",
        is_final=True,
        names={
            "en": "Reference exchange rates of the euro",
            "fr": "Taux de change de référence de l'euro",
        },
        frequencies_available=["D", "M"],
        default_frequency="D",
        time_coverage=TimeCoverage(),
        dimensions=[
            CatalogDimension(
                id="FREQ",
                position=0,
                codes=[CodeRef(id="D"), CodeRef(id="M")],
                total_codes=2,
            ),
            CatalogDimension(
                id="EXR_CURRENCY",
                position=1,
                codes=[CodeRef(id="USD"), CodeRef(id="GBP"), CodeRef(id="JPY")],
                total_codes=3,
            ),
        ],
        primary_measure="OBS_VALUE",
        key_template="{FREQ}.{EXR_CURRENCY}",
        common_queries=[
            CommonQuery(label="Daily USD", key="D.USD", params={"lastNObservations": 10}),
        ],
    )


def _fake_hicp_entry() -> EnrichedDataflow:
    return EnrichedDataflow(
        agency="BE2",
        id="DF_HICP_2025",
        version="1.0",
        category="prices",
        is_final=True,
        names={"en": "Inflation and harmonised consumer price index"},
        frequencies_available=["M"],
        default_frequency="M",
        time_coverage=TimeCoverage(),
        dimensions=[],
        primary_measure="OBS_VALUE",
        key_template=None,
        common_queries=[],
    )


def _fake_catalog() -> Catalog:
    return Catalog(entries=[_fake_exr_entry(), _fake_hicp_entry()])


def _load_cassette(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _make_services(handler) -> Services:
    settings = Settings(
        api_base_url="https://nsidisseminate-stat.nbb.be/rest",
        http_cache_enabled=False,
        retry_attempts=1,
        retry_wait_initial=0.0,
        retry_wait_max=0.0,
        rate_limit_requests=1000,
        rate_limit_period=1,
    )
    client = NBBClient(settings=settings, transport=httpx.MockTransport(handler))
    svc = Services(settings=settings, client=client, catalog=_fake_catalog())
    return svc


@pytest.fixture
def services_with_data_handler(data_payload):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(data_payload).encode(),
            headers={"content-type": "application/vnd.sdmx.data+json; version=2.0"},
        )

    svc = _make_services(handler)
    reset_services(svc)
    yield svc
    reset_services(None)


@pytest.fixture
def services_with_capture():
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["accept"] = request.headers.get("accept", "")
        payload = _load_cassette("data_DF_EXR.json")
        return httpx.Response(
            200,
            content=json.dumps(payload).encode(),
            headers={"content-type": "application/vnd.sdmx.data+json; version=2.0"},
        )

    svc = _make_services(handler)
    reset_services(svc)
    yield svc, captured
    reset_services(None)


# ---------------------------------------------------------------------------
# nbb_search
# ---------------------------------------------------------------------------


async def test_search_returns_exr_for_english_query(services_with_data_handler):
    r = await search_tool.run("exchange rate")
    assert r["count"] >= 1
    top_ids = [x["id"] for x in r["results"]]
    assert "DF_EXR" in top_ids
    exr = next(x for x in r["results"] if x["id"] == "DF_EXR")
    assert exr["category"] == "exchange_interest_rates"
    assert exr["name"].startswith("Reference exchange")
    assert exr["dimensions"][0]["id"] == "FREQ"
    assert exr["common_queries"]


async def test_search_french_localised_name(services_with_data_handler):
    r = await search_tool.run("change", language="fr")
    exr = next((x for x in r["results"] if x["id"] == "DF_EXR"), None)
    assert exr is not None
    assert "Taux" in exr["name"]


async def test_search_respects_category_filter(services_with_data_handler):
    r = await search_tool.run("index", category="prices")
    assert all(x["category"] == "prices" for x in r["results"])


# ---------------------------------------------------------------------------
# nbb_describe
# ---------------------------------------------------------------------------


async def test_describe_local_returns_full_fiche(services_with_data_handler):
    r = await describe_tool.run("DF_EXR", agency="BE2")
    assert r["id"] == "DF_EXR"
    assert r["category"] == "exchange_interest_rates"
    assert r["key_template"] == "{FREQ}.{EXR_CURRENCY}"
    assert r["live_refresh"] is None
    assert len(r["dimensions"]) == 2
    assert r["dimensions"][0]["codes"] is not None


async def test_describe_include_codes_false(services_with_data_handler):
    r = await describe_tool.run("DF_EXR", include_codes=False)
    for d in r["dimensions"]:
        assert d["codes"] is None


async def test_describe_unknown_raises(services_with_data_handler):
    with pytest.raises(NBBCatalogError):
        await describe_tool.run("DF_NOPE")


async def test_describe_force_refresh_calls_api(dataflow_detail_payload):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            content=json.dumps(dataflow_detail_payload).encode(),
            headers={"content-type": "application/vnd.sdmx.structure+json; version=1.0"},
        )

    svc = _make_services(handler)
    reset_services(svc)
    try:
        r = await describe_tool.run("DF_EXR", force_refresh=True)
        assert calls["n"] == 1
        assert r["live_refresh"] is not None
        assert r["live_refresh"]["structure"]["primary_measure"] == "OBS_VALUE"
    finally:
        reset_services(None)


# ---------------------------------------------------------------------------
# nbb_query
# ---------------------------------------------------------------------------


async def test_query_with_filters_resolves_key(services_with_capture):
    _svc, captured = services_with_capture
    r = await query_tool.run("DF_EXR", filters={"FREQ": "D", "EXR_CURRENCY": "USD"})
    assert "data/BE2,DF_EXR,1.0/D.USD" in captured["url"]
    assert r["metadata"]["key"] == "D.USD"
    assert r["summary"]["series_count"] >= 1


async def test_query_with_raw_key(services_with_capture):
    _svc, captured = services_with_capture
    await query_tool.run("DF_EXR", key="D.GBP")
    assert "data/BE2,DF_EXR,1.0/D.GBP" in captured["url"]


async def test_query_unknown_dimension_raises(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await query_tool.run("DF_EXR", filters={"BAD": "X"})
    assert exc.value.code == "UNKNOWN_DIMENSION"


async def test_query_conflicting_args_raises(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await query_tool.run("DF_EXR", key="D.USD", filters={"FREQ": "D"})
    assert exc.value.code == "CONFLICTING_ARGS"


async def test_query_empty_filters_uses_all(services_with_capture):
    _svc, captured = services_with_capture
    await query_tool.run("DF_EXR")
    assert "data/BE2,DF_EXR,1.0/all" in captured["url"]


async def test_query_respects_max_observations(services_with_capture):
    r = await query_tool.run("DF_EXR", max_observations=2)
    assert len(r["data"]) <= 2
    assert r.get("truncated") is True or r["summary"]["total_observations"] == len(r["data"])


# ---------------------------------------------------------------------------
# nbb_quick
# ---------------------------------------------------------------------------


async def test_quick_exchange_rate_builds_currency_key(services_with_capture):
    _svc, captured = services_with_capture
    r = await quick_tool.run("exchange_rate", currency="USD")
    assert "data/BE2,DF_EXR,1.0/D.USD" in captured["url"]
    assert r["topic"] == "exchange_rate"


async def test_quick_exchange_rate_respects_frequency(services_with_capture):
    _svc, captured = services_with_capture
    await quick_tool.run("exchange_rate", currency="GBP", frequency="M")
    assert "data/BE2,DF_EXR,1.0/M.GBP" in captured["url"]


async def test_quick_unknown_topic_lists_valid_ones(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await quick_tool.run("not_a_topic")
    assert exc.value.code == "UNKNOWN_TOPIC"
    assert "exchange_rate" in exc.value.details["valid_topics"]


async def test_quick_missing_target_flow_raises(services_with_data_handler):
    # DF_IRESCB isn't in our tiny fake catalog → get() raises.
    with pytest.raises(NBBCatalogError):
        await quick_tool.run("policy_rate")


# ---------------------------------------------------------------------------
# nbb_compare
# ---------------------------------------------------------------------------


async def test_compare_requires_min_two_series(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await compare_tool.run([{"dataflow_id": "DF_EXR"}])
    assert exc.value.code == "TOO_FEW_SERIES"


async def test_compare_rejects_too_many_series(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await compare_tool.run([{"dataflow_id": "DF_EXR"}] * 6)
    assert exc.value.code == "TOO_MANY_SERIES"


async def test_compare_aligns_two_series(services_with_data_handler):
    r = await compare_tool.run(
        [
            {"dataflow_id": "DF_EXR", "label": "EXR1"},
            {"dataflow_id": "DF_EXR", "label": "EXR2"},
        ]
    )
    assert "EXR1" in r["series_labels"]
    assert "EXR2" in r["series_labels"]
    assert r["period_count"] > 0
    row = r["data"][0]
    assert "period" in row
    assert "EXR1" in row
    assert "EXR2" in row


async def test_compare_rejects_missing_dataflow_id(services_with_data_handler):
    with pytest.raises(NBBValidationError) as exc:
        await compare_tool.run([{"label": "x"}, {"dataflow_id": "DF_EXR"}])
    assert exc.value.code == "MISSING_DATAFLOW_ID"


# ---------------------------------------------------------------------------
# nbb_status
# ---------------------------------------------------------------------------


async def test_status_snapshot(services_with_data_handler):
    r = await status_tool.run()
    assert "version" in r
    assert r["catalog"]["dataflow_count"] == 2
    assert "exchange_interest_rates" in r["catalog"]["categories"]
    assert r["api"]["base_url"].startswith("https://")
    # Cache may or may not exist on the runner's home dir — just assert the
    # shape of the reported stats.
    assert "entries" in r["cache"]
    assert "size_mb" in r["cache"]
