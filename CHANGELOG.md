# Changelog

All notable changes to `mcp-nbb` are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

Breaking: requires `mcp` 2.x; `nbb_quick` returns one headline series per
topic; `nbb_compare` rejects specs matching several series. Next release
should be 0.2.0.

### Fixed
- **Fresh installs were broken**: `mcp>=1.0.0` resolved to mcp 2.x, where
  `mcp.server.fastmcp` no longer exists, so `mcp-nbb` 0.1.2 crashed on import.
  Migrated to `MCPServer` (mcp 2.x) and pinned `mcp>=2.3.0,<3`.
- Tool errors lost their actionable content: only the bare message reached the
  model (and mcp 2.x would have replaced it with a generic text). `NBBError`
  is now converted to `ToolError`/`ResourceError` and its text carries the
  `[CODE]` plus a `details` JSON (valid dimensions, codes, `did_you_mean`).
- Token budget not enforced: `max_observations` was ignored by
  `format="series"`, and per-series stats were unbounded
  (`nbb_quick("gdp")` returned ~62k tokens). New `max_series` cap (default 20)
  and truncation hints naming the oldest returned period.
- Catalogue frequencies were wrong for every flow: `FREQ` listed the generic
  `CL_FREQ` codelist, so `default_frequency` was `D` everywhere (GDP included)
  and `time_coverage` was always empty. Fiches are now enriched from the
  `availableconstraint` endpoint: only codes that hold data, real frequencies,
  time coverage and observation count (schema version 2).
- `nbb_quick` ignored `frequency` on every topic but `exchange_rate`, silently
  ignored `currency`, and fetched whole flows (539 series for `gdp`). Each
  topic now targets a verified headline series; `currency` on another topic is
  an error.
- `nbb_compare`: quarterly→semester and weekly periods were bucketed wrongly,
  daily→weekly did not aggregate, an invalid `frequency` was accepted, upsampling
  was silently attempted, and an ambiguous spec silently used the first of
  N series. Added a `max_periods` row cap.
- `nbb_status`: API call counters were never updated, and `version` reported
  0.1.0 (now read from package metadata).
- `NBB_LOG_LEVEL` / `NBB_LOG_FORMAT` were ignored by the server.
- Settings were frozen at import time and malformed `NBB_*` values silently
  fell back to defaults; they are now read per instance and raise `ConfigError`.
- Corrupt disk-cache entries raised instead of being treated as misses;
  expired entries are now removed on read.
- `build_catalog`: `--only`/`--limit` overwrote `_index.json` with the partial
  selection; failed fiches with an existing file were missing from
  `_build_errors.json`; added `--timeout` (default 120 s) for the large
  external-trade flows.
- `format="table"` was identical to `summary`; it now omits per-series stats.
- Unknown category resource now errors with the valid list instead of
  rendering an empty page.

### Changed
- Catalogue refreshed: 223 flows (adds `DF_IROLOYLD_2026`, `IMF_YGG`),
  ~6 MB instead of ~9 MB. Coverage/observation-count changes alone no longer
  count as drift (refreshed at most every 90 days) to keep weekly PRs quiet.
- Tools declare MCP annotations (`read_only_hint`, `open_world_hint`).
- Dropped unused dependencies `cachetools` and `python-dateutil`; raised
  floors to tested versions (verified with `uv --resolution lowest-direct`).
- CI: Python 3.11–3.14 matrix, `mypy --strict` (was declared but never run,
  12 errors), lowest-deps job, catalogue/wheel checks derived from
  `_index.json` instead of a hard-coded 221, actions bumped to latest majors
  and pinned by SHA, least-privilege `permissions`.
- The weekly catalogue workflow now runs the test suite before opening its PR
  (PRs opened with `GITHUB_TOKEN` never trigger CI).

## [0.1.2] — 2026-04-12

### Fixed
- Disk cache now completely bypasses lookup/store when the configured TTL
  is `<= 0`, fixing a flaky `test_disk_cache_respects_ttl` on Windows
  Python 3.11 where `time.time()` timer resolution could return equal
  values on back-to-back calls.

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

[Unreleased]: https://github.com/lacausecrypto/mcp-nbb/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/lacausecrypto/mcp-nbb/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/lacausecrypto/mcp-nbb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lacausecrypto/mcp-nbb/releases/tag/v0.1.0
