"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import platformdirs

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _default_cache_path() -> Path:
    """Return the OS-appropriate cache file path.

    Resolves to:
    - ``~/.cache/mcp-nbb/http.db`` on Linux
    - ``~/Library/Caches/mcp-nbb/http.db`` on macOS
    - ``%LOCALAPPDATA%\\mcp-nbb\\Cache\\http.db`` on Windows
    """
    return Path(platformdirs.user_cache_dir("mcp-nbb", "mcp-nbb")) / "http.db"


def _env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    val = os.getenv(key)
    try:
        return float(val) if val is not None else default
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    api_base_url: str = os.getenv("NBB_API_BASE_URL", "https://nsidisseminate-stat.nbb.be/rest")
    api_timeout: float = _env_float("NBB_API_TIMEOUT", 30.0)
    user_agent: str = os.getenv("NBB_USER_AGENT", DEFAULT_USER_AGENT)
    origin: str = os.getenv("NBB_ORIGIN", "https://dataexplorer.nbb.be")

    http_cache_enabled: bool = _env_bool("NBB_HTTP_CACHE_ENABLED", True)
    http_cache_path: Path = (
        Path(os.path.expanduser(os.environ["NBB_HTTP_CACHE_PATH"]))
        if os.getenv("NBB_HTTP_CACHE_PATH")
        else _default_cache_path()
    )

    memory_cache_ttl_data: int = _env_int("NBB_MEMORY_CACHE_TTL_DATA", 300)
    memory_cache_ttl_structure: int = _env_int("NBB_MEMORY_CACHE_TTL_STRUCTURE", 3600)

    rate_limit_requests: int = _env_int("NBB_RATE_LIMIT_REQUESTS", 100)
    rate_limit_period: int = _env_int("NBB_RATE_LIMIT_PERIOD", 60)

    retry_attempts: int = _env_int("NBB_RETRY_ATTEMPTS", 3)
    retry_wait_initial: float = _env_float("NBB_RETRY_WAIT_INITIAL", 1.0)
    retry_wait_max: float = _env_float("NBB_RETRY_WAIT_MAX", 30.0)

    log_level: str = os.getenv("NBB_LOG_LEVEL", "INFO")
    log_format: str = os.getenv("NBB_LOG_FORMAT", "json")


def get_settings() -> Settings:
    return Settings()
