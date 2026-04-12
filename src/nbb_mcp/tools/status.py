"""``nbb_status`` — diagnostic snapshot of the MCP server (no API call)."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .. import __version__
from ..services import get_services


def _disk_cache_stats(cache_root: Path) -> dict[str, Any]:
    disk_dir = cache_root.parent / "disk"
    if not disk_dir.is_dir():
        return {"path": str(disk_dir), "entries": 0, "size_mb": 0.0, "exists": False}
    files = [f for f in disk_dir.iterdir() if f.is_file() and f.suffix == ".json"]
    size_bytes = sum(f.stat().st_size for f in files)
    return {
        "path": str(disk_dir),
        "exists": True,
        "entries": len(files),
        "size_mb": round(size_bytes / (1024 * 1024), 3),
    }


async def run() -> dict[str, Any]:
    svc = get_services()
    cat = svc.catalog
    meta = cat.index_metadata() or {}

    return {
        "version": __version__,
        "uptime_seconds": round(time.time() - svc.stats.started_at, 1),
        "catalog": {
            "dataflow_count": len(cat),
            "categories": cat.categories(),
            "agencies": cat.agencies(),
            "built_at": meta.get("built_at"),
            "schema_version": meta.get("schema_version"),
        },
        "cache": _disk_cache_stats(svc.settings.http_cache_path),
        "api": {
            "base_url": svc.settings.api_base_url,
            "user_agent": svc.settings.user_agent[:80] + "…"
            if len(svc.settings.user_agent) > 80
            else svc.settings.user_agent,
            "origin": svc.settings.origin,
            "call_count": svc.stats.api_call_count,
            "last_call_at": svc.stats.last_api_call_at,
            "last_error": svc.stats.last_api_error,
        },
    }
