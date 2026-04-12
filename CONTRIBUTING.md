# Contributing to mcp-nbb

Thanks for your interest in improving `mcp-nbb`! This document covers the
essentials.

## Development setup

```bash
git clone https://github.com/lacausecrypto/mcp-nbb.git
cd mcp-nbb
python -m venv .venv              # or: uv venv
.venv/bin/pip install -e ".[dev]" # or: uv sync --all-extras
```

### Running tests

```bash
.venv/bin/pytest                     # full unit + integration suite
.venv/bin/pytest -m e2e              # end-to-end MCP stdio tests
.venv/bin/pytest -m "not e2e"        # skip slower tests
.venv/bin/pytest --cov=nbb_mcp       # with coverage
```

All PRs must keep the test suite green. E2E tests spawn the server as a
subprocess — they run on macOS, Linux and Windows.

### Lint

```bash
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
```

### Rebuilding the bundled catalogue

The 221 enriched fiches under `src/nbb_mcp/data/catalog/` are regenerated
from the live NBB API:

```bash
.venv/bin/mcp-nbb-build-catalog --force
```

Only commit catalogue changes when there is a real upstream diff — the weekly
`build-catalog.yml` GitHub Action does this automatically.

## Reporting bugs

Use the **Bug report** issue template. Please include:

- Python version (`python --version`) and OS.
- `mcp-nbb` version (`pip show mcp-nbb`).
- Minimal reproduction (exact tool call with parameters).
- Relevant logs (`NBB_LOG_LEVEL=DEBUG NBB_LOG_FORMAT=console`).

## Proposing changes

1. **Open an issue first** for anything larger than a small bug fix. This
   avoids duplicated work and gets alignment on the approach.
2. **Keep PRs focused.** One feature / fix per PR.
3. **Add or update tests.** New tools or parser branches need coverage; bug
   fixes need a regression test.
4. **Update `CHANGELOG.md`** under the `## [Unreleased]` section.
5. **Run ruff + pytest** locally before pushing.

## Code style

- Python 3.11+ type hints everywhere, `from __future__ import annotations` at
  the top of each module.
- Pydantic v2 models for all public data shapes.
- Structured errors (subclasses of `NBBError`) with LLM-actionable `details`.
- No comments that narrate *what* the code does — only *why* when non-obvious.
- `ruff format` for formatting.

## Commit messages

Imperative mood, concise subject (<72 chars), optional body explaining *why*.
Examples:

```
fix(client): strip content-encoding before persisting to disk cache
feat(quick): add 'consumer_credit' topic mapped to DF_CRECONSURV
docs(readme): clarify Windows cache path
```

## License

By contributing you agree that your contributions will be licensed under the
MIT License (see `LICENSE`).
