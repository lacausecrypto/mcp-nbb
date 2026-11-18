"""Runtime configuration loaded from environment variables.

The environment is read when :class:`Settings` is instantiated (not at import
time), and malformed values raise instead of silently falling back to a default.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

import platformdirs

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_LOG_FORMATS = frozenset({"json", "console"})

_Num = TypeVar("_Num", int, float)


class ConfigError(ValueError):
    """An ``NBB_*`` environment variable holds an unusable value."""


def _default_cache_path() -> Path:
    """Return the OS-appropriate cache file path.

    Resolves to:
    - ``~/.cache/mcp-nbb/http.db`` on Linux
    - ``~/Library/Caches/mcp-nbb/http.db`` on macOS
    - ``%LOCALAPPDATA%\\mcp-nbb\\Cache\\http.db`` on Windows
    """
    return Path(platformdirs.user_cache_dir("mcp-nbb", "mcp-nbb")) / "http.db"


def _env_str(key: str, default: str) -> str:
    val = os.getenv(key)
    return val if val else default


def _env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    norm = val.strip().lower()
    if norm in _TRUE:
        return True
    if norm in _FALSE:
        return False
    raise ConfigError(f"{key}={val!r} is not a boolean (expected one of {sorted(_TRUE | _FALSE)})")


def _env_number(key: str, default: _Num, cast: type[_Num], *, minimum: _Num) -> _Num:
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    try:
        parsed = cast(val.strip())
    except ValueError as exc:
        raise ConfigError(f"{key}={val!r} is not a valid {cast.__name__}") from exc
    if parsed < minimum:
        raise ConfigError(f"{key}={parsed} must be >= {minimum}")
    return parsed


def _env_choice(key: str, default: str, choices: frozenset[str], *, upper: bool) -> str:
    raw = _env_str(key, default).strip()
    val = raw.upper() if upper else raw.lower()
    if val not in choices:
        raise ConfigError(f"{key}={raw!r} must be one of {sorted(choices)}")
    return val


def _cache_path() -> Path:
    raw = os.getenv("NBB_HTTP_CACHE_PATH")
    return Path(os.path.expanduser(raw)) if raw else _default_cache_path()


@dataclass(frozen=True)
class Settings:
    api_base_url: str = field(
        default_factory=lambda: _env_str(
            "NBB_API_BASE_URL", "https://nsidisseminate-stat.nbb.be/rest"
        )
    )
    api_timeout: float = field(
        default_factory=lambda: _env_number("NBB_API_TIMEOUT", 30.0, float, minimum=0.1)
    )
    user_agent: str = field(default_factory=lambda: _env_str("NBB_USER_AGENT", DEFAULT_USER_AGENT))
    origin: str = field(
        default_factory=lambda: _env_str("NBB_ORIGIN", "https://dataexplorer.nbb.be")
    )

    http_cache_enabled: bool = field(
        default_factory=lambda: _env_bool("NBB_HTTP_CACHE_ENABLED", True)
    )
    http_cache_path: Path = field(default_factory=_cache_path)

    memory_cache_ttl_data: int = field(
        default_factory=lambda: _env_number("NBB_MEMORY_CACHE_TTL_DATA", 300, int, minimum=0)
    )
    memory_cache_ttl_structure: int = field(
        default_factory=lambda: _env_number("NBB_MEMORY_CACHE_TTL_STRUCTURE", 3600, int, minimum=0)
    )

    rate_limit_requests: int = field(
        default_factory=lambda: _env_number("NBB_RATE_LIMIT_REQUESTS", 100, int, minimum=1)
    )
    rate_limit_period: int = field(
        default_factory=lambda: _env_number("NBB_RATE_LIMIT_PERIOD", 60, int, minimum=1)
    )

    retry_attempts: int = field(
        default_factory=lambda: _env_number("NBB_RETRY_ATTEMPTS", 3, int, minimum=1)
    )
    retry_wait_initial: float = field(
        default_factory=lambda: _env_number("NBB_RETRY_WAIT_INITIAL", 1.0, float, minimum=0.0)
    )
    retry_wait_max: float = field(
        default_factory=lambda: _env_number("NBB_RETRY_WAIT_MAX", 30.0, float, minimum=0.0)
    )

    log_level: str = field(
        default_factory=lambda: _env_choice("NBB_LOG_LEVEL", "INFO", _LOG_LEVELS, upper=True)
    )
    log_format: str = field(
        default_factory=lambda: _env_choice("NBB_LOG_FORMAT", "json", _LOG_FORMATS, upper=False)
    )


def get_settings() -> Settings:
    return Settings()
