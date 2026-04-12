"""Tests for NBBClient against mocked transports using real NBB fixtures."""

from __future__ import annotations

import httpx
import pytest

from nbb_mcp.client import NBBClient, build_flow_ref
from nbb_mcp.models.errors import NBBConnectionError


def test_build_flow_ref_uses_commas():
    assert build_flow_ref("BE2", "DF_EXR", "1.0") == "BE2,DF_EXR,1.0"
    assert build_flow_ref("IMF", "CPI") == "IMF,CPI,1.0"


async def test_list_dataflows_returns_payload(test_settings, dataflow_list_payload):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["accept"] = request.headers.get("accept", "")
        import json as _json

        return httpx.Response(
            200,
            content=_json.dumps(dataflow_list_payload).encode(),
            headers={"content-type": "application/vnd.sdmx.structure+json; version=1.0"},
        )

    client = NBBClient(settings=test_settings, transport=httpx.MockTransport(handler))
    result = await client.list_dataflows()
    await client.aclose()

    assert "dataflow/all/all/latest" in captured["url"]
    assert "references=none" in captured["url"]
    assert "detail=allstubs" in captured["url"]
    assert captured["accept"].startswith("application/vnd.sdmx.structure+json")
    assert len(result["data"]["dataflows"]) == 221


async def test_fetch_data_builds_comma_ref_url(test_settings, data_payload):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        import json as _json

        return httpx.Response(
            200,
            content=_json.dumps(data_payload).encode(),
            headers={"content-type": "application/vnd.sdmx.data+json; version=2.0"},
        )

    client = NBBClient(settings=test_settings, transport=httpx.MockTransport(handler))
    await client.fetch_data("BE2", "DF_EXR", "1.0", "all", last_n_observations=3)
    await client.aclose()

    assert "data/BE2,DF_EXR,1.0/all" in captured["url"]
    assert "lastNObservations=3" in captured["url"]


async def test_connection_error_raises_typed(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    client = NBBClient(settings=test_settings, transport=httpx.MockTransport(handler))
    with pytest.raises(NBBConnectionError):
        await client.list_dataflows()
    await client.aclose()


async def test_retry_recovers_after_transient(test_settings):
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(
                500, content=b"boom", headers={"content-type": "text/plain"}
            )
        return httpx.Response(
            200,
            content=b'{"data":{"dataflows":[]},"meta":{}}',
            headers={"content-type": "application/vnd.sdmx.structure+json; version=1.0"},
        )

    settings = type(test_settings)(
        **{**test_settings.__dict__, "retry_attempts": 3}
    )
    client = NBBClient(settings=settings, transport=httpx.MockTransport(handler))
    result = await client.list_dataflows()
    await client.aclose()
    assert attempts["n"] == 2
    assert result == {"data": {"dataflows": []}, "meta": {}}
