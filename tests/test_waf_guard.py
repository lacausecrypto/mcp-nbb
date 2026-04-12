"""Tests for the WAF / content-type guard (silent-200 HTML responses)."""

from __future__ import annotations

import httpx
import pytest

from nbb_mcp.client import NBBClient
from nbb_mcp.models.errors import (
    NBBNotFoundError,
    NBBParseError,
    NBBValidationError,
    NBBWAFBlockError,
)

WAF_HTML = (
    '<html><body><script type="text/javascript">'
    'window.location = "https://pub.nbb.be/api/inf/Error/NotAvailable.aspx?errorid=123";'
    "</script></body></html>"
)


def _make_client(handler, test_settings):
    transport = httpx.MockTransport(handler)
    return NBBClient(settings=test_settings, transport=transport)


async def test_waf_html_200_raises_waf_block(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            content=WAF_HTML.encode(),
            headers={"content-type": "text/html; charset=utf-8"},
        )

    client = _make_client(handler, test_settings)
    with pytest.raises(NBBWAFBlockError) as exc_info:
        await client.list_dataflows()
    assert exc_info.value.code == "WAF_BLOCK"
    assert "NotAvailable" in str(exc_info.value.details) or exc_info.value.details
    await client.aclose()


async def test_waf_html_any_status_raises_waf_block(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=302,
            content=b"<html><body>redirect</body></html>",
            headers={"content-type": "text/html"},
        )

    client = _make_client(handler, test_settings)
    with pytest.raises(NBBWAFBlockError):
        await client.list_dataflows()
    await client.aclose()


async def test_404_text_plain_maps_to_not_found(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=404,
            content=b"NoResultsFound",
            headers={"content-type": "text/plain"},
        )

    client = _make_client(handler, test_settings)
    with pytest.raises(NBBNotFoundError) as exc_info:
        await client.list_dataflows()
    assert "NoResultsFound" in exc_info.value.message
    await client.aclose()


async def test_400_text_plain_maps_to_validation(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=400,
            content=b"Invalid version string provided",
            headers={"content-type": "text/plain"},
        )

    client = _make_client(handler, test_settings)
    with pytest.raises(NBBValidationError):
        await client.list_dataflows()
    await client.aclose()


async def test_unexpected_content_type_raises_parse_error(test_settings):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            content=b"{}",
            headers={"content-type": "application/json"},  # not vnd.sdmx.*
        )

    client = _make_client(handler, test_settings)
    with pytest.raises(NBBParseError):
        await client.list_dataflows()
    await client.aclose()


async def test_waf_required_headers_sent(test_settings):
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        for k, v in request.headers.items():
            received[k.lower()] = v
        return httpx.Response(
            status_code=200,
            content=b'{"data":{"dataflows":[]},"meta":{}}',
            headers={"content-type": "application/vnd.sdmx.structure+json; version=1.0"},
        )

    client = _make_client(handler, test_settings)
    await client.list_dataflows()
    await client.aclose()

    assert "user-agent" in received
    assert "Mozilla/5.0" in received["user-agent"]
    assert received.get("origin") == "https://dataexplorer.nbb.be"
    assert received.get("accept", "").startswith("application/vnd.sdmx.structure+json")
