"""MCP server for the National Bank of Belgium SDMX statistical API."""

from importlib.metadata import version as _dist_version

# Single source of truth: the installed distribution metadata (pyproject.toml).
__version__ = _dist_version("mcp-nbb")
