"""Service singleton shared by all MCP tools and resources.

The MCP server (FastMCP) constructs a :class:`Services` instance once at startup
via its ``lifespan`` hook and stores it in a module-level singleton. Tools access
it through :func:`get_services` — no explicit injection is required on every call.

Tests can override the singleton with :func:`reset_services` to inject a fake
:class:`Services` backed by a mocked :class:`NBBClient` and a temp-dir catalogue.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .catalog import Catalog
from .client import NBBClient
from .config import Settings, get_settings


@dataclass
class ServiceStats:
    started_at: float = field(default_factory=time.time)
    api_call_count: int = 0
    last_api_call_at: float | None = None
    last_api_error: str | None = None


class Services:
    """Holder for all long-lived resources shared between MCP handlers."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        client: NBBClient | None = None,
        catalog: Catalog | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or NBBClient(settings=self.settings)
        self.catalog = catalog or Catalog.load()
        self.stats = ServiceStats()

    async def close(self) -> None:
        await self.client.aclose()


_singleton: Services | None = None


def get_services() -> Services:
    global _singleton
    if _singleton is None:
        _singleton = Services()
    return _singleton


async def close_services() -> None:
    global _singleton
    if _singleton is not None:
        await _singleton.close()
        _singleton = None


def reset_services(svc: Services | None) -> None:
    """Test helper: replace the singleton with ``svc`` (or clear it if ``None``)."""
    global _singleton
    _singleton = svc


__all__ = [
    "ServiceStats",
    "Services",
    "close_services",
    "get_services",
    "reset_services",
]
