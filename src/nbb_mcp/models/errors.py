"""Error hierarchy for the NBB MCP server."""

from __future__ import annotations

from typing import Any


class NBBError(Exception):
    """Base error for all NBB MCP failures."""

    def __init__(
        self,
        message: str,
        code: str = "NBB_ERROR",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}(code={self.code!r}, message={self.message!r})"

    def to_dict(self) -> dict[str, Any]:
        return {"error": type(self).__name__, "code": self.code, "message": self.message, "details": self.details}


class NBBConnectionError(NBBError):
    """Network-level failure (DNS, TCP, TLS, 5xx)."""


class NBBTimeoutError(NBBError):
    """Request exceeded the configured timeout."""


class NBBRateLimitError(NBBError):
    """Upstream rate limit hit (HTTP 429) or self-imposed limit exceeded."""


class NBBNotFoundError(NBBError):
    """Requested resource (dataflow, key) does not exist."""


class NBBValidationError(NBBError):
    """Input parameters failed validation (HTTP 400 or local)."""


class NBBParseError(NBBError):
    """Response could not be parsed as expected SDMX payload."""


class NBBWAFBlockError(NBBError):
    """WAF returned an HTML redirect page instead of an SDMX response."""


class NBBCatalogError(NBBError):
    """Local catalogue is missing, corrupted, or inconsistent."""
