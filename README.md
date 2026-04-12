# mcp-nbb

<!-- mcp-name: io.github.lacausecrypto/mcp-nbb -->

[![CI](https://github.com/lacausecrypto/mcp-nbb/actions/workflows/ci.yml/badge.svg)](https://github.com/lacausecrypto/mcp-nbb/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/mcp-nbb.svg)](https://pypi.org/project/mcp-nbb/)
[![Python versions](https://img.shields.io/pypi/pyversions/mcp-nbb?cacheSeconds=300)](https://pypi.org/project/mcp-nbb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![MCP compatible](https://img.shields.io/badge/MCP-compatible-blue)](https://modelcontextprotocol.io)

**MCP server for the [National Bank of Belgium](https://www.nbb.be/) SDMX statistical API.**

Exposes the 221 NBB dataflows (194 BE2 + 27 IMF/SDDS) as **6 LLM-friendly
tools** and **3 browsable resources**, with a bundled enriched catalogue so
the LLM can discover, describe, and query dataflows without redundant API
calls.

- **Upstream**: `https://nsidisseminate-stat.nbb.be/rest` (NSI Web Service v8)
- **Transport**: stdio (standard MCP)
- **Python**: 3.11+
- **Platforms**: Linux, macOS, Windows
- **221 dataflows** classified into 14 categories — see [DATAFLOWS_CATALOG.md](DATAFLOWS_CATALOG.md)

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

The package ships with the full enriched catalogue (~9 MB under
`src/nbb_mcp/data/catalog/`). No build step is required for regular use.

---

## Claude Desktop configuration

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` on
macOS, `%APPDATA%\Claude\claude_desktop_config.json` on Windows, or the
equivalent on Linux.

### Using `uvx` (recommended once published to PyPI)

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

### From a local editable install

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

---

## Tools

| Tool | API calls | Purpose |
|---|---|---|
| `nbb_search(query, …)` | 0 | Fuzzy search over the 221 local fiches (en/fr/nl/de). |
| `nbb_describe(dataflow_id, …)` | 0 (default) | Full enriched fiche — dimensions, codelists, key template, common queries. `force_refresh=True` revalidates live. |
| `nbb_query(dataflow_id, key=…, filters=…)` | 1 | Generic data fetch. Either `key` (raw SDMX) or `filters` (`{"FREQ":"D","EXR_CURRENCY":"USD"}`). |
| `nbb_quick(topic, …)` | 1 | Topic-based shortcut for 18 common queries — see topic table below. |
| `nbb_compare(series, …)` | N | Align 2-5 series on a common time index, downsampling finer frequencies via closing aggregation. |
| `nbb_status()` | 0 | Diagnostic snapshot: catalog, cache, API config. |

### `nbb_quick` topics

| Topic | Dataflow | Parameters |
|---|---|---|
| `exchange_rate` | `BE2/DF_EXR` | `currency`, `frequency` |
| `policy_rate` | `BE2/DF_IRESCB` | — |
| `mortgage_rate` | `BE2/DF_MIR` | — |
| `long_term_yield` | `BE2/DF_IROLOYLD` | — |
| `inflation_hicp` | `BE2/DF_HICP_2025` | — |
| `inflation_national` | `BE2/DF_NICP_2025` | — |
| `ppi` | `BE2/DF_PPI` | — |
| `industrial_production` | `BE2/DF_INDPROD` | — |
| `gdp` / `gdp_growth` | `BE2/DF_QNA_DISS` | — |
| `unemployment_rate` | `BE2/DF_UNEMPLOY_RATE` | — |
| `employment` | `BE2/DF_EMPLOY_DISS` | — |
| `government_debt` | `BE2/DF_CGD` | — |
| `government_deficit` | `BE2/DF_NFGOV_NET_DISS` | — |
| `current_account` | `BE2/DF_BOPBPM6` | — |
| `consumer_confidence` | `BE2/DF_CONSN` | — |
| `business_confidence` | `BE2/DF_BUSSURVM` | — |
| `trade_balance` | `BE2/DF_EXTERNAL_TRADE_OVERVIEW` | — |

### Resources

| URI | Content |
|---|---|
| `nbb://catalog` | Markdown index of all 221 dataflows by category. |
| `nbb://dataflow/{agency}/{dataflow_id}` | Full enriched fiche for one flow. |
| `nbb://category/{category}` | All flows in a category. |

---

## Example prompts in Claude

> "What's the EUR/USD exchange rate over the last month?"
> → `nbb_quick("exchange_rate", currency="USD", frequency="D", last_n_observations=30)`

> "Compare Belgian GDP growth to the unemployment rate since 2020."
> → `nbb_compare([{dataflow_id:"DF_QNA_DISS",label:"GDP"}, {dataflow_id:"DF_UNEMPLOY_RATE",label:"Unemployment"}], start_period="2020-Q1")`

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
| `NBB_RETRY_ATTEMPTS` | `3` | Retries on transient errors |
| `NBB_LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `NBB_LOG_FORMAT` | `json` | `json` or `console` |

The default cache path resolves to:
- **Linux**: `~/.cache/mcp-nbb/`
- **macOS**: `~/Library/Caches/mcp-nbb/`
- **Windows**: `%LOCALAPPDATA%\mcp-nbb\Cache\`

---

## Refreshing the catalogue

The bundled `src/nbb_mcp/data/catalog/` snapshot is regenerated by fetching
the DSD + codelists for each of the 221 dataflows:

```bash
mcp-nbb-build-catalog --force
```

Options:

- `--force` — rebuild every fiche, ignoring existing ones.
- `--limit N` — only process the first N flows (debug).
- `--only BE2/DF_EXR,BE2/DF_HICP_2025` — rebuild specific flows.
- `--concurrency 5` — parallel DSD requests.

A full rebuild takes ~80 seconds against the live API. Catalogue footprint is
capped at ~9 MB by truncating codelists to 200 codes per dimension (some IMF
flows have 65 000+ codes).

A weekly GitHub Action (`build-catalog.yml`) rebuilds the catalogue and opens
a PR if drift is detected.

---

## Troubleshooting

### "WAF returned an HTML redirect"

The NBB API is behind a WAF that returns an HTML 200 redirect for any request
without a browser-like `User-Agent` and an `Origin: https://dataexplorer.nbb.be`
header. The client injects both by default. If you override `NBB_USER_AGENT`,
keep a genuine-looking browser string.

### "HTTP 404 NoResultsFound" on a data query

The SDMX key didn't match any series. Use `nbb_describe(dataflow_id)` to see
valid codes, or pass `filters={}` / `key="all"` to retrieve everything and
then narrow via `start_period`/`end_period`.

### "Too many observations, truncated"

Every data response is capped at `max_observations=200` by default. Increase
via `nbb_query(max_observations=1000)` or narrow the query with a period
window.

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
ruff check src tests
mcp-nbb-build-catalog      # refresh the bundled catalogue
mcp-nbb                    # run the server (stdio)
```

CI runs on Linux, macOS and Windows with Python 3.11 and 3.12. See
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
