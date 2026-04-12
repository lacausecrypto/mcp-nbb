"""Tests for the disk cache transport."""

from __future__ import annotations

import httpx

from nbb_mcp.client import NBBClient, _DiskCacheTransport


def _sdmx_json_response(body: bytes = b'{"data":{"dataflows":[]},"meta":{}}') -> httpx.Response:
    return httpx.Response(
        200,
        content=body,
        headers={"content-type": "application/vnd.sdmx.structure+json; version=1.0"},
    )


async def test_disk_cache_second_call_served_from_cache(test_settings, tmp_path):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return _sdmx_json_response()

    inner = httpx.MockTransport(handler)
    cache_transport = _DiskCacheTransport(
        inner,
        cache_dir=tmp_path / "disk",
        structure_ttl=3600,
        data_ttl=300,
    )
    settings = type(test_settings)(**{**test_settings.__dict__, "http_cache_enabled": True})
    client = NBBClient(settings=settings, transport=cache_transport)
    await client.list_dataflows()
    await client.list_dataflows()
    await client.aclose()

    assert calls["n"] == 1, "second call should be served from disk cache"


async def test_disk_cache_survives_new_client(test_settings, tmp_path):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return _sdmx_json_response()

    cache_dir = tmp_path / "disk"

    inner1 = httpx.MockTransport(handler)
    client1 = NBBClient(
        settings=test_settings,
        transport=_DiskCacheTransport(
            inner1, cache_dir=cache_dir, structure_ttl=3600, data_ttl=300
        ),
    )
    await client1.list_dataflows()
    await client1.aclose()

    # Second client, same disk cache dir, same MockTransport counter reset.
    inner2 = httpx.MockTransport(handler)
    client2 = NBBClient(
        settings=test_settings,
        transport=_DiskCacheTransport(
            inner2, cache_dir=cache_dir, structure_ttl=3600, data_ttl=300
        ),
    )
    await client2.list_dataflows()
    await client2.aclose()

    assert calls["n"] == 1, "cache should persist across NBBClient instances"


async def test_disk_cache_respects_ttl(test_settings, tmp_path):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return _sdmx_json_response()

    cache = _DiskCacheTransport(
        httpx.MockTransport(handler),
        cache_dir=tmp_path / "disk",
        structure_ttl=0,  # immediately stale
        data_ttl=0,
    )
    client = NBBClient(settings=test_settings, transport=cache)
    await client.list_dataflows()
    await client.list_dataflows()
    await client.aclose()

    assert calls["n"] == 2, "TTL=0 should bypass the cache"


async def test_disk_cache_skips_non_sdmx_responses(test_settings, tmp_path):
    """WAF HTML must never be cached."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            content=b"<html>waf</html>",
            headers={"content-type": "text/html"},
        )

    cache = _DiskCacheTransport(
        httpx.MockTransport(handler),
        cache_dir=tmp_path / "disk",
        structure_ttl=3600,
        data_ttl=300,
    )
    client = NBBClient(settings=test_settings, transport=cache)
    # Both calls will raise WAFBlock — catch and carry on.
    import pytest

    from nbb_mcp.models.errors import NBBWAFBlockError

    with pytest.raises(NBBWAFBlockError):
        await client.list_dataflows()
    with pytest.raises(NBBWAFBlockError):
        await client.list_dataflows()
    await client.aclose()

    assert calls["n"] == 2, "WAF responses must never be cached"
