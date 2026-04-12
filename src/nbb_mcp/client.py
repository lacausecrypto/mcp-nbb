"""HTTP client for the NBB SDMX REST API.

Responsibilities:
- Inject the WAF-required headers (``Origin``, browser ``User-Agent``) on every request.
- Reject silent-200 HTML responses from the WAF (``NBBWAFBlockError``).
- Persist a simple TTL disk cache (survives MCP restarts).
- Token-bucket rate limit outgoing requests (aiolimiter).
- Retry transient failures with exponential backoff + jitter (stamina).

Design note on caching:
    The NBB upstream sends ``Cache-Control: no-cache; no-store; max-age=0;
    must-revalidate`` (with non-RFC semicolon separators) and no ``ETag`` or
    ``Last-Modified`` headers. Conditional GET is therefore impossible against this
    server — we own the caching policy. The disk cache below applies a TTL per
    endpoint family (``structure`` vs ``data``) independently of upstream directives,
    which is the only sane choice for static reference data.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import stamina
from aiolimiter import AsyncLimiter

from .config import Settings, get_settings
from .logging import get_logger
from .models.errors import (
    NBBConnectionError,
    NBBNotFoundError,
    NBBParseError,
    NBBRateLimitError,
    NBBTimeoutError,
    NBBValidationError,
    NBBWAFBlockError,
)

log = get_logger("nbb_mcp.client")

SDMX_STRUCTURE_JSON = "application/vnd.sdmx.structure+json"
SDMX_DATA_JSON = "application/vnd.sdmx.data+json"
SDMX_CONTENT_TYPE_PREFIX = "application/vnd.sdmx."

STRUCTURE_SEGMENTS = ("/dataflow/", "/datastructure/", "/codelist/", "/conceptscheme/")
DATA_SEGMENT = "/data/"


def build_flow_ref(agency: str, dataflow_id: str, version: str = "1.0") -> str:
    """Build an NSI v8 flow reference (comma-separated, not slash)."""
    return f"{agency},{dataflow_id},{version}"


def _ttl_for_path(path: str, structure_ttl: int, data_ttl: int) -> int | None:
    if any(seg in path for seg in STRUCTURE_SEGMENTS):
        return structure_ttl
    if DATA_SEGMENT in path:
        return data_ttl
    return None


class _DiskCacheTransport(httpx.AsyncBaseTransport):
    """Persistent TTL cache transport, one JSON file per (URL + Accept) key.

    File format::

        {
          "url": "...",
          "accept": "...",
          "status": 200,
          "headers": [["content-type", "application/vnd.sdmx.structure+json"]],
          "body_b64": "...",
          "stored_at": 1713...,
          "ttl": 3600
        }

    Entries past their TTL are treated as a miss (and overwritten on refresh).
    Only GET requests are cached. Only 200 responses with an SDMX content-type
    are persisted (we never cache WAF HTML or errors).
    """

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        *,
        cache_dir: Path,
        structure_ttl: int,
        data_ttl: int,
    ) -> None:
        self._inner = inner
        self._cache_dir = cache_dir
        self._structure_ttl = structure_ttl
        self._data_ttl = data_ttl
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _key(self, request: httpx.Request) -> Path:
        # Key by method + URL + negotiated content types. Accept-Language MUST
        # participate because NBB returns localized names per language.
        raw = "\n".join(
            [
                request.method,
                str(request.url),
                request.headers.get("accept", ""),
                request.headers.get("accept-language", ""),
            ]
        )
        digest = hashlib.sha256(raw.encode()).hexdigest()[:32]
        return self._cache_dir / f"{digest}.json"

    def _load(self, path: Path, ttl: int) -> httpx.Response | None:
        if not path.is_file():
            return None
        try:
            entry = json.loads(path.read_text())
        except (OSError, ValueError):
            return None
        stored_at = float(entry.get("stored_at", 0))
        if (time.time() - stored_at) > ttl:
            return None
        import base64

        body = base64.b64decode(entry.get("body_b64", ""))
        headers = [(k, v) for k, v in entry.get("headers", [])]
        resp = httpx.Response(
            status_code=int(entry.get("status", 200)),
            headers=headers,
            content=body,
        )
        resp.extensions["from_cache"] = True
        return resp

    _STRIP_HEADERS = frozenset({"content-encoding", "transfer-encoding", "content-length"})

    def _store(
        self, path: Path, request: httpx.Request, response: httpx.Response, ttl: int
    ) -> None:
        import base64

        # ``response.content`` is already decoded. Persist only headers that don't
        # imply an encoding we no longer apply (gzip/br/chunked), else httpx will
        # try to re-decode the stored decoded bytes and fail.
        headers = [
            (k, v) for k, v in response.headers.items() if k.lower() not in self._STRIP_HEADERS
        ]
        entry = {
            "url": str(request.url),
            "accept": request.headers.get("accept", ""),
            "status": response.status_code,
            "headers": headers,
            "body_b64": base64.b64encode(response.content).decode("ascii"),
            "stored_at": time.time(),
            "ttl": ttl,
        }
        try:
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(entry))
            tmp.replace(path)
        except OSError as exc:
            log.warning("nbb.cache.store_failed", path=str(path), error=str(exc))

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            return await self._inner.handle_async_request(request)

        ttl = _ttl_for_path(request.url.path, self._structure_ttl, self._data_ttl)
        if ttl is None:
            return await self._inner.handle_async_request(request)

        key = self._key(request)
        cached = self._load(key, ttl)
        if cached is not None:
            return cached

        response = await self._inner.handle_async_request(request)
        # Drain the stream so we can both persist and return the bytes.
        await response.aread()
        ct = response.headers.get("content-type", "")
        if response.status_code == 200 and ct.startswith(SDMX_CONTENT_TYPE_PREFIX):
            self._store(key, request, response, ttl)
        response.extensions["from_cache"] = False
        return response

    async def aclose(self) -> None:
        await self._inner.aclose()


def _build_transport(settings: Settings) -> httpx.AsyncBaseTransport:
    base: httpx.AsyncBaseTransport = httpx.AsyncHTTPTransport(retries=0)
    if not settings.http_cache_enabled:
        return base
    cache_dir: Path = settings.http_cache_path.parent / "disk"
    return _DiskCacheTransport(
        base,
        cache_dir=cache_dir,
        structure_ttl=settings.memory_cache_ttl_structure,
        data_ttl=settings.memory_cache_ttl_data,
    )


def _check_response(r: httpx.Response) -> None:
    """Raise on WAF HTML, unexpected content types, and HTTP error statuses.

    Order matters: WAF HTML check runs even on HTTP 200.
    """
    ct = r.headers.get("content-type", "").lower()
    body_head = r.text[:256] if r.content else ""

    if not ct.startswith(SDMX_CONTENT_TYPE_PREFIX):
        if r.status_code == 404:
            raise NBBNotFoundError(
                body_head.strip() or "Not found",
                code="NOT_FOUND",
                details={"url": str(r.url), "status": r.status_code},
            )
        if r.status_code == 400:
            raise NBBValidationError(
                body_head.strip() or "Bad request",
                code="BAD_REQUEST",
                details={"url": str(r.url), "status": r.status_code},
            )
        if "<html" in body_head.lower() or "NotAvailable" in body_head:
            raise NBBWAFBlockError(
                "WAF returned an HTML redirect — check User-Agent and Origin headers.",
                code="WAF_BLOCK",
                details={"url": str(r.url), "content_type": ct, "status": r.status_code},
            )
        if r.status_code >= 500:
            raise NBBConnectionError(
                f"Upstream server error ({r.status_code}): {body_head[:120]}",
                code=f"HTTP_{r.status_code}",
                details={"url": str(r.url)},
            )
        if r.status_code == 429:
            raise NBBRateLimitError(
                "Rate limit hit",
                code="RATE_LIMIT",
                details={"url": str(r.url)},
            )
        raise NBBParseError(
            f"Unexpected content type '{ct}' (status={r.status_code})",
            code="UNEXPECTED_CONTENT_TYPE",
            details={"url": str(r.url), "body_head": body_head[:200]},
        )

    if r.status_code >= 400:
        if r.status_code == 404:
            raise NBBNotFoundError(body_head.strip(), code="NOT_FOUND", details={"url": str(r.url)})
        if r.status_code == 400:
            raise NBBValidationError(
                body_head.strip(), code="BAD_REQUEST", details={"url": str(r.url)}
            )
        if r.status_code == 429:
            raise NBBRateLimitError(
                "Rate limit hit", code="RATE_LIMIT", details={"url": str(r.url)}
            )
        raise NBBConnectionError(
            f"HTTP {r.status_code}", code=f"HTTP_{r.status_code}", details={"url": str(r.url)}
        )


class NBBClient:
    """Async HTTP client for the NBB SDMX REST endpoint."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        headers = {
            "User-Agent": self.settings.user_agent,
            "Origin": self.settings.origin,
            "Accept-Language": "en",
        }
        self._client = httpx.AsyncClient(
            base_url=self.settings.api_base_url,
            headers=headers,
            timeout=httpx.Timeout(self.settings.api_timeout),
            transport=transport or _build_transport(self.settings),
            follow_redirects=False,
        )
        self._limiter = AsyncLimiter(
            max_rate=self.settings.rate_limit_requests,
            time_period=self.settings.rate_limit_period,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> NBBClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        accept: str,
        params: Mapping[str, Any] | None = None,
        language: str | None = None,
    ) -> httpx.Response:
        attempts = max(1, self.settings.retry_attempts)
        retry_on = (NBBConnectionError, NBBTimeoutError, NBBRateLimitError)

        async for attempt in stamina.retry_context(
            on=retry_on,
            attempts=attempts,
            wait_initial=self.settings.retry_wait_initial,
            wait_max=self.settings.retry_wait_max,
            wait_jitter=2.0,
        ):
            with attempt:
                async with self._limiter:
                    request_headers: dict[str, str] = {"Accept": accept}
                    if language:
                        request_headers["Accept-Language"] = language
                    try:
                        response = await self._client.request(
                            method,
                            url,
                            params=dict(params) if params else None,
                            headers=request_headers,
                        )
                    except httpx.TimeoutException as exc:
                        raise NBBTimeoutError(
                            f"Request timed out after {self.settings.api_timeout}s",
                            code="TIMEOUT",
                            details={"url": url},
                        ) from exc
                    except httpx.TransportError as exc:
                        raise NBBConnectionError(
                            f"Transport error: {exc}",
                            code="TRANSPORT",
                            details={"url": url},
                        ) from exc

                    from_cache = response.extensions.get("from_cache", False)
                    log.info(
                        "nbb.request",
                        method=method,
                        url=str(response.url),
                        status=response.status_code,
                        content_type=response.headers.get("content-type", ""),
                        from_cache=from_cache,
                        bytes=len(response.content),
                    )

                    _check_response(response)
                    return response

        raise NBBConnectionError(
            "Exhausted retries", code="RETRIES_EXHAUSTED", details={"url": url}
        )

    async def get_structure(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        language: str | None = None,
    ) -> dict[str, Any]:
        """GET an SDMX-JSON structure response (structure+json)."""
        response = await self._request(
            "GET", path, accept=SDMX_STRUCTURE_JSON, params=params, language=language
        )
        try:
            return response.json()
        except ValueError as exc:
            raise NBBParseError("Invalid JSON in structure response", code="JSON_DECODE") from exc

    async def get_data(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET an SDMX-JSON data response (data+json)."""
        response = await self._request("GET", path, accept=SDMX_DATA_JSON, params=params)
        try:
            return response.json()
        except ValueError as exc:
            raise NBBParseError("Invalid JSON in data response", code="JSON_DECODE") from exc

    # Convenience wrappers -------------------------------------------------

    async def list_dataflows(self, *, language: str | None = None) -> dict[str, Any]:
        return await self.get_structure(
            "dataflow/all/all/latest",
            params={"references": "none", "detail": "allstubs"},
            language=language,
        )

    async def get_dataflow(
        self,
        agency: str,
        dataflow_id: str,
        version: str = "1.0",
        *,
        references: str = "all",
        language: str | None = None,
    ) -> dict[str, Any]:
        return await self.get_structure(
            f"dataflow/{agency}/{dataflow_id}/{version}",
            params={"references": references},
            language=language,
        )

    async def fetch_data(
        self,
        agency: str,
        dataflow_id: str,
        version: str,
        key: str = "all",
        *,
        start_period: str | None = None,
        end_period: str | None = None,
        last_n_observations: int | None = None,
        first_n_observations: int | None = None,
    ) -> dict[str, Any]:
        ref = build_flow_ref(agency, dataflow_id, version)
        params: dict[str, Any] = {}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if last_n_observations is not None:
            params["lastNObservations"] = last_n_observations
        if first_n_observations is not None:
            params["firstNObservations"] = first_n_observations
        return await self.get_data(f"data/{ref}/{key}", params=params or None)


@asynccontextmanager
async def nbb_client(settings: Settings | None = None) -> AsyncIterator[NBBClient]:
    client = NBBClient(settings=settings)
    try:
        yield client
    finally:
        await client.aclose()


__all__ = [
    "SDMX_DATA_JSON",
    "SDMX_STRUCTURE_JSON",
    "NBBClient",
    "_DiskCacheTransport",
    "_check_response",
    "build_flow_ref",
    "nbb_client",
]
