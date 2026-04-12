# Security Policy

## Supported versions

Only the latest minor release line receives security fixes.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security reports.**

Instead, use GitHub's private [Security Advisories](https://github.com/lacausecrypto/mcp-nbb/security/advisories/new)
form, or email the maintainer directly.

Please include:

- A description of the issue and its potential impact.
- Steps to reproduce (minimal proof-of-concept if possible).
- Affected versions.
- Any suggested mitigation.

You should receive an initial acknowledgement within **72 hours**. We aim to
ship a fix within **14 days** for high-severity issues.

## Scope

This project is a client for a public statistical API. The main attack
surfaces to consider are:

- **Input handling in tool parameters** (SDMX keys, filter dicts, topic names).
- **Response parsing** (malicious or malformed SDMX-JSON could crash the
  parser — we have a structural guard and typed errors).
- **Cache poisoning** (the persistent L1 disk cache rejects non-SDMX
  responses, but a malicious proxy could in theory manipulate cached fiches).
- **The bundled catalogue** (a tampered fiche in `src/nbb_mcp/data/catalog/`
  could mislead the LLM). Verify your install source.

Out of scope:

- Vulnerabilities in the upstream NBB API itself.
- Reports that simply expose the hardcoded WAF-compatible `User-Agent`/`Origin`
  headers (these are necessary for the server to function; see README).
