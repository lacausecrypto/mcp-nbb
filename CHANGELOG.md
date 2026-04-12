# Changelog

All notable changes to `mcp-nbb` are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.1] — 2026-04-12

### Added
- `<!-- mcp-name: io.github.lacausecrypto/mcp-nbb -->` marker in the README
  so the official MCP Registry can verify PyPI package ownership.
- `server.json` manifest at the repo root for Registry publication via
  `mcp-publisher`.

## [0.1.0] — 2026-04-12

Initial public release.

### Added
- **Validated upstream endpoint**: `https://nsidisseminate-stat.nbb.be/rest`
  (NSI Web Service v8) with the mandatory WAF headers (`Origin`, browser
  `User-Agent`).
- **Async `NBBClient`** on top of `httpx` with:
  - WAF guard: rejects silent-200 HTML redirects with `NBBWAFBlockError`.
  - Persistent TTL disk cache (custom transport, ~90 LOC). The upstream sends a
    non-RFC `Cache-Control` with no `ETag`, making off-the-shelf HTTP caches
    incompatible. Cache persists across restarts; key includes
    `Accept-Language` so multilingual responses don't collide.
  - Cross-platform cache path via `platformdirs` (Linux / macOS / Windows).
  - Rate limiting (`aiolimiter`) and exponential retry with jitter (`stamina`).
- **Minimal SDMX-JSON parsers** for structure (v1.0) and data (v2.0) responses,
  as served by NSI v8.
- **Enriched catalogue builder** (`mcp-nbb-build-catalog`):
  - Fetches all 221 dataflows in parallel, merges multilingual names across
    en/fr/nl/de, classifies into 14 categories, generates key templates and
    common queries, writes one JSON fiche per flow.
  - Truncates oversized codelists to 200 codes per dimension.
  - Final bundle: ~9 MB / 221 fiches, shipped inside the wheel.
- **`Catalog` loader with hybrid fuzzy search**
  (`max(token_set_ratio, 0.85 · partial_ratio)`), sub-millisecond on 221
  entries.
- **FastMCP server** exposing 6 tools and 3 resource URIs:
  - `nbb_search`, `nbb_describe`, `nbb_query`, `nbb_quick`, `nbb_compare`,
    `nbb_status`.
  - `nbb://catalog`, `nbb://dataflow/{agency}/{id}`, `nbb://category/{name}`.
- **`nbb_quick` topic routing** for 18 well-known flows (exchange rate,
  inflation, GDP, unemployment, public debt, etc.).
- **`nbb_compare`** with frequency downsampling (closing-last aggregation).
- **LLM-friendly output formatter** with per-series stats and
  `max_observations` token budget guard.
- **Structured error hierarchy** with LLM-actionable details (valid
  dimensions, valid codes sample, key template).
- **Test suite**: 120 tests green on Linux, macOS and Windows (unit,
  integration, E2E stdio, bundled-catalogue assertions).
- **CI/CD**:
  - `ci.yml` — matrix (3 OS × 2 Python) + ruff + wheel build with bundled
    catalogue verification.
  - `build-catalog.yml` — weekly cron that opens a PR if the upstream drifts.
  - `release.yml` — PyPI publish via trusted publishing on GitHub release.
  - `dependabot.yml` — weekly dependency updates.
- **Community files**: `LICENSE` (MIT), `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, issue and PR templates.

[Unreleased]: https://github.com/lacausecrypto/mcp-nbb/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/lacausecrypto/mcp-nbb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lacausecrypto/mcp-nbb/releases/tag/v0.1.0
