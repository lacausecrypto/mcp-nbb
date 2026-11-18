# mcp-nbb

<!-- mcp-name: io.github.lacausecrypto/mcp-nbb -->

[![CI](https://github.com/lacausecrypto/mcp-nbb/actions/workflows/ci.yml/badge.svg)](https://github.com/lacausecrypto/mcp-nbb/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/mcp-nbb.svg)](https://pypi.org/project/mcp-nbb/)
[![Python versions](https://img.shields.io/pypi/pyversions/mcp-nbb?cacheSeconds=300)](https://pypi.org/project/mcp-nbb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![MCP compatible](https://img.shields.io/badge/MCP-compatible-blue)](https://modelcontextprotocol.io)

**MCP server for the [National Bank of Belgium](https://www.nbb.be/) SDMX statistical API.**

Exposes the 223 NBB dataflows (195 BE2 + 28 IMF/SDDS) as **6 LLM-friendly
tools** and **3 browsable resources**, with a bundled enriched catalogue so
the LLM can discover, describe, and query dataflows without redundant API
calls.

- **Upstream**: `https://nsidisseminate-stat.nbb.be/rest` (NSI Web Service v8)
- **Transport**: stdio (standard MCP)
- **Python**: 3.11 – 3.14
- **MCP SDK**: `mcp` 2.x
- **Platforms**: Linux, macOS, Windows
- **223 dataflows** classified into 14 categories — see [DATAFLOWS_CATALOG.md](DATAFLOWS_CATALOG.md)

---

## Install

### From PyPI (recommended)

```bash
# With uv (runs without installing globally)
uvx mcp-nbb

# Or install into a regular venv
pip install mcp-nbb
```

### From source

```bash
git clone https://github.com/lacausecrypto/mcp-nbb.git
cd mcp-nbb
pip install -e .
```

The package ships with the full enriched catalogue (~6 MB under
`src/nbb_mcp/data/catalog/`). No build step is required for regular use.

> **Upgrading from 0.1.x**: 0.1.2 cannot start on a fresh install because
> `mcp` 2.x removed the `FastMCP` module it imported. Version 0.2.0 targets
> `mcp` 2.x; run `uvx mcp-nbb@latest` or `pip install -U mcp-nbb`.

---

## Client configuration

### Claude Code

```bash
claude mcp add nbb -- uvx mcp-nbb
```

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` on
macOS, `%APPDATA%\Claude\claude_desktop_config.json` on Windows, or the
equivalent on Linux.

#### Using `uvx` (recommended)

```json
{
  "mcpServers": {
    "nbb": {
      "command": "uvx",
      "args": ["mcp-nbb"]
    }
  }
}
```

#### From a local editable install

**macOS / Linux**:

```json
{
  "mcpServers": {
    "nbb": {
      "command": "/Users/you/projects/mcp-nbb/.venv/bin/mcp-nbb"
    }
  }
}
```

**Windows**:

```json
{
  "mcpServers": {
    "nbb": {
      "command": "C:\\Users\\you\\projects\\mcp-nbb\\.venv\\Scripts\\mcp-nbb.exe"
    }
  }
}
```

Restart Claude Desktop — the 6 `nbb_*` tools appear in the MCP panel.
All tools are read-only; the catalogue tools (`nbb_search`, `nbb_describe`,
`nbb_status`) never leave the process, which the tool annotations declare.

---

## Tools

| Tool | API calls | Purpose |
|---|---|---|
| `nbb_search(query, …)` | 0 | Fuzzy search over the local fiches (en/fr/nl/de), with available frequencies and time coverage. |
| `nbb_describe(dataflow_id, …)` | 0 (default) | Full enriched fiche — dimensions with the codes that actually hold data, frequencies, time coverage, key template, common queries. `force_refresh=True` revalidates live. |
| `nbb_query(dataflow_id, key=…, filters=…)` | 1 | Generic data fetch. Either `key` (raw SDMX) or `filters` (`{"FREQ":"D","EXR_CURRENCY":"USD"}`). Bounded by `max_observations` (rows) and `max_series` (per-series stats). |
| `nbb_quick(topic, …)` | 1 | Headline series for 18 common topics — see topic table below. Last 24 observations unless a period window is given. |
| `nbb_compare(series, …)` | N | Align 2-5 single series on a common time index, downsampling finer frequencies via closing aggregation (`max_periods` rows). |
| `nbb_status()` | 0 | Diagnostic snapshot: catalog, cache, API config and per-process usage counters. |

### `nbb_quick` topics

Every topic resolves to one headline series. `frequency` (A/S/Q/M/D) overrides
the default on any topic and is validated against the frequencies the flow
actually publishes; `currency` only applies to `exchange_rate`.

| Topic | Dataflow | Headline series (default frequency) |
|---|---|---|
| `exchange_rate` | `BE2/DF_EXR` | EUR vs `currency` (default USD) (D) |
| `policy_rate` | `BE2/DF_IRESCB` | ECB deposit facility rate (D) |
| `mortgage_rate` | `BE2/DF_MIR` | Cost of borrowing, households, house purchase (M) |
| `long_term_yield` | `BE2/DF_IROLOYLD` | Belgian 10-year OLO yield (D) |
| `inflation_hicp` | `BE2/DF_HICP_2025` | HICP all items, % change (M) |
| `inflation_national` | `BE2/DF_NICP_2025` | National CPI general index, % change (M) |
| `ppi` | `BE2/DF_PPI` | Producer prices, general index, total market (M) |
| `industrial_production` | `BE2/DF_INDPROD` | Total industry B-D, calendar adjusted (M) |
| `gdp` / `gdp_growth` | `BE2/DF_QNA_DISS` | GDP chain-linked volume / q-o-q %, SA (Q) |
| `unemployment_rate` | `BE2/DF_UNEMPLOY_RATE` | Harmonised rate, total, SA (M) |
| `employment` | `BE2/DF_EMPLOY_DISS` | Total domestic employment, SA (Q) |
| `government_debt` | `BE2/DF_CGD` | General government consolidated gross debt (Q) |
| `government_deficit` | `BE2/DF_NFGOV_NET_DISS` | Net lending/borrowing, % of GDP (A) |
| `current_account` | `BE2/DF_BOPBPM6` | Current account, net (Q) |
| `consumer_confidence` | `BE2/DF_CONSN` | Consumer confidence indicator (M) |
| `business_confidence` | `BE2/DF_BUSSURVM` | Business barometer, smoothed (M) |
| `trade_balance` | `BE2/DF_EXTERNAL_TRADE_OVERVIEW` | Goods trade balance with the world (M) |

### Responses and errors

Data tools return `summary` (series count, total observations, period range),
`series` (per-series stats: first/latest, min/max/mean, change), `data` (rows)
and `metadata`. Two budgets keep a response inside the model's context:
`max_observations` (rows, default 200) and `max_series` (per-series stats,
default 20). When either one cuts, `truncated` is true and `next_page_hint`
says why and names the oldest returned period. `format` is `summary`
(default), `table` (rows without per-series stats) or `series` (rows grouped
by series).

Errors come back as tool errors whose text starts with a code and carries the
data needed to fix the call:

```text
[UNKNOWN_CODE] Unknown code(s) ['D'] for dimension 'FREQ' on BE2/DF_QNA_DISS
details: {"dimension": "FREQ", "invalid_codes": ["D"], "valid_codes_sample": ["A", "Q"], ...}
```

Main codes: `UNKNOWN_FLOW` (with `did_you_mean`), `UNKNOWN_DIMENSION`,
`UNKNOWN_CODE`, `UNKNOWN_TOPIC`, `AMBIGUOUS_SERIES`, `FREQUENCY_TOO_FINE`,
`INVALID_FORMAT`, `NOT_FOUND`, `TIMEOUT`, `WAF_BLOCK`.

### Resources

| URI | Content |
|---|---|
| `nbb://catalog` | Markdown index of all dataflows by category. |
| `nbb://dataflow/{agency}/{dataflow_id}` | Full enriched fiche for one flow. |
| `nbb://category/{category}` | All flows in a category. |

---

## Example prompts in Claude

> "What's the EUR/USD exchange rate over the last month?"
> → `nbb_quick("exchange_rate", currency="USD", frequency="D", last_n_observations=30)`

> "Compare Belgian GDP growth to the unemployment rate since 2020."
> → `nbb_compare([{dataflow_id:"DF_QNA_DISS",key:"Q.2.B1GM.VZ.LQ.Y",label:"GDP q/q"}, {dataflow_id:"DF_UNEMPLOY_RATE",key:"M.AA.Z0000.Y.BE.HUR.RATE",label:"Unemployment"}], start_period="2020-Q1")`
> (each spec must match exactly one series — an ambiguous key returns `AMBIGUOUS_SERIES` with sample keys)

> "Find NBB dataflows about consumer credit."
> → `nbb_search("consumer credit")` → `nbb_describe(...)` → `nbb_query(...)`.

---

## Configuration (environment variables)

All settings have sensible defaults; override via environment variables.

| Variable | Default | Purpose |
|---|---|---|
| `NBB_API_BASE_URL` | `https://nsidisseminate-stat.nbb.be/rest` | SDMX REST base URL |
| `NBB_API_TIMEOUT` | `30` | Per-request timeout (s) |
| `NBB_USER_AGENT` | browser UA | **Required for the WAF** — default is a valid Chrome UA string |
| `NBB_ORIGIN` | `https://dataexplorer.nbb.be` | **Required for the WAF** |
| `NBB_HTTP_CACHE_ENABLED` | `true` | Persistent disk cache |
| `NBB_HTTP_CACHE_PATH` | OS cache dir | Override cache location (defaults to `platformdirs.user_cache_dir`) |
| `NBB_MEMORY_CACHE_TTL_DATA` | `300` | TTL for data responses (s) |
| `NBB_MEMORY_CACHE_TTL_STRUCTURE` | `3600` | TTL for structure responses (s) |
| `NBB_RATE_LIMIT_REQUESTS` | `100` | Self-imposed rate limit (req/period) |
| `NBB_RATE_LIMIT_PERIOD` | `60` | Rate limit window (s) |
| `NBB_RETRY_ATTEMPTS` | `3` | Attempts on transient errors (>= 1) |
| `NBB_RETRY_WAIT_INITIAL` | `1.0` | First backoff delay (s) |
| `NBB_RETRY_WAIT_MAX` | `30.0` | Maximum backoff delay (s) |
| `NBB_LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `NBB_LOG_FORMAT` | `json` | `json` or `console` |

An invalid value (`NBB_API_TIMEOUT=abc`, `NBB_RETRY_ATTEMPTS=0`,
`NBB_LOG_LEVEL=LOUD`…) stops the server with a `ConfigError` naming the
variable instead of silently falling back to the default. Logs go to stderr;
stdout is reserved for the MCP protocol.

The default cache path resolves to:
- **Linux**: `~/.cache/mcp-nbb/`
- **macOS**: `~/Library/Caches/mcp-nbb/`
- **Windows**: `%LOCALAPPDATA%\mcp-nbb\Cache\`

---

## Refreshing the catalogue

The bundled `src/nbb_mcp/data/catalog/` snapshot is regenerated by fetching,
for each dataflow, the DSD + codelists and the actual content constraint
(`availableconstraint`): codes that hold data, frequencies, time coverage and
observation count:

```bash
mcp-nbb-build-catalog --force
```

Options:

- `--force` — rebuild every fiche, ignoring existing ones.
- `--limit N` — only process the first N flows (debug).
- `--only BE2/DF_EXR,BE2/DF_HICP_2025` — rebuild specific flows.
- `--concurrency 5` — parallel DSD requests.
- `--timeout 120` — per-request timeout (availability queries on the large
  external-trade flows exceed the interactive 30 s default).

A full rebuild takes ~6 minutes against the live API. Catalogue footprint is
~6 MB: codelists are narrowed to codes that hold data and truncated to 200
codes per dimension.

A weekly GitHub Action (`build-catalog.yml`) rebuilds the catalogue, runs the
full test suite on it, and opens a PR if structural drift is detected
(coverage/observation counts alone refresh at most every 90 days).

---

## Troubleshooting

### "WAF returned an HTML redirect"

The NBB API is behind a WAF that returns an HTML 200 redirect for any request
without a browser-like `User-Agent` and an `Origin: https://dataexplorer.nbb.be`
header. The client injects both by default. If you override `NBB_USER_AGENT`,
keep a genuine-looking browser string.

### "HTTP 404 NoResultsFound" or an empty result

The SDMX key didn't match any series in that period. `nbb_describe(dataflow_id)`
lists, per dimension, only the codes that hold data, plus the flow's
`time_coverage` — a valid code combination can still be empty, and periods
outside the coverage return nothing. Empty results include a `next_page_hint`
with the coverage and key template.

### "truncated": true

Every data response is capped at `max_observations=200` rows and
`max_series=20` per-series stats by default. `next_page_hint` names the oldest
returned period; pass an earlier `end_period`, raise the caps, or narrow the
key. Errors carry a `[CODE]` and a `details` JSON (valid dimensions, codes,
`did_you_mean`) the model can act on.

### Catalogue not found

If you run without the bundled `src/nbb_mcp/data/catalog/`, run
`mcp-nbb-build-catalog` once to populate it.

---

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full dev workflow. Short form:

```bash
pip install -e ".[dev]"
pytest                     # full suite (unit + integration + E2E)
pytest -m "not e2e"        # fast subset
ruff check src tests && ruff format --check src tests
mypy src                   # strict
mcp-nbb-build-catalog      # refresh the bundled catalogue
mcp-nbb                    # run the server (stdio)
```

CI runs on Linux, macOS and Windows with Python 3.11 – 3.14, plus a job on the
lowest declared dependency versions, `ruff` + `mypy --strict`, and a wheel
check that the bundled catalogue matches `_index.json`. See
[DATAFLOWS_CATALOG.md](DATAFLOWS_CATALOG.md) for the classified inventory.

---

## Security

Please report vulnerabilities privately — see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) — see `LICENSE` for the full text.

## Disclaimer

This project is **not affiliated with or endorsed by** the National Bank of
Belgium. It is an independent client of their public SDMX REST API. The
browser-like `User-Agent` and `Origin` headers are required by the upstream
WAF and used solely to access public statistical data. Users are responsible
for complying with NBB's terms of use.
