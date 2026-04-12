"""Shared pytest fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from nbb_mcp.client import NBBClient
from nbb_mcp.config import Settings

CASSETTES = Path(__file__).parent / "cassettes"


def _load_bytes(name: str) -> bytes:
    return (CASSETTES / name).read_bytes()


def _load_json(name: str) -> dict:
    return json.loads(_load_bytes(name).decode("utf-8"))


@pytest.fixture
def dataflow_list_payload() -> dict:
    return _load_json("dataflow_list.json")


@pytest.fixture
def dataflow_detail_payload() -> dict:
    return _load_json("dataflow_detail_DF_EXR.json")


@pytest.fixture
def data_payload() -> dict:
    return _load_json("data_DF_EXR.json")


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    """A Settings instance with caching disabled and tmp-path paths."""
    return Settings(
        api_base_url="https://nsidisseminate-stat.nbb.be/rest",
        api_timeout=10.0,
        http_cache_enabled=False,
        http_cache_path=tmp_path / "http.db",
        rate_limit_requests=1000,
        rate_limit_period=1,
        retry_attempts=1,
        retry_wait_initial=0.0,
        retry_wait_max=0.0,
    )


def make_mock_client(
    handler: httpx.MockTransport,
    settings: Settings,
) -> NBBClient:
    """Build an NBBClient backed by an httpx MockTransport (no network)."""
    return NBBClient(settings=settings, transport=handler)
