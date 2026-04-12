"""Markdown renderers for the three ``nbb://`` MCP resources.

These helpers are pure — they take an :class:`EnrichedDataflow` or a
:class:`Catalog` and return an LLM-ready markdown string. The FastMCP
resource decorators in ``server.py`` wire URIs to these renderers.
"""

from __future__ import annotations

from ..catalog import Catalog
from ..models.catalog import EnrichedDataflow


def render_catalog_summary(cat: Catalog, *, language: str = "en") -> str:
    lines = ["# NBB Catalog", ""]
    lines.append(f"- **Dataflows**: {len(cat)}")
    lines.append(f"- **Agencies**: {', '.join(f'{a} ({n})' for a, n in cat.agencies().items())}")
    lines.append("")
    lines.append("## Categories")
    lines.append("")
    for cat_name, count in cat.categories().items():
        lines.append(f"- `{cat_name}` — {count} flows (`nbb://category/{cat_name}`)")
    lines.append("")
    lines.append("## Top-level flows")
    lines.append("")
    lines.append("| Agency | ID | Category | Name |")
    lines.append("|---|---|---|---|")
    for e in cat.list_dataflows(include_non_final=False)[:60]:
        name = e.names.get(language) or e.names.get("en") or e.id
        lines.append(f"| `{e.agency}` | `{e.id}` | {e.category} | {name} |")
    if len(cat) > 60:
        lines.append("")
        lines.append(f"_…and {len(cat) - 60} more. Use `nbb_search` to find specific flows._")
    return "\n".join(lines)


def render_dataflow(entry: EnrichedDataflow, *, language: str = "en") -> str:
    name = entry.names.get(language) or entry.names.get("en") or entry.id
    lines = [f"# {entry.agency}/{entry.id} — {name}", ""]
    lines.append(f"- **Category**: `{entry.category}`")
    lines.append(f"- **Version**: `{entry.version}`")
    lines.append(f"- **Primary measure**: `{entry.primary_measure}`")
    if entry.default_frequency:
        lines.append(f"- **Default frequency**: `{entry.default_frequency}`")
    if entry.frequencies_available:
        lines.append(
            f"- **Frequencies**: {', '.join(f'`{f}`' for f in entry.frequencies_available)}"
        )
    if entry.key_template:
        lines.append(f"- **Key template**: `{entry.key_template}`")

    lines.append("")
    lines.append("## Localised names")
    lines.append("")
    for lang, n in entry.names.items():
        lines.append(f"- **{lang}**: {n}")

    lines.append("")
    lines.append("## Dimensions")
    lines.append("")
    if not entry.dimensions:
        lines.append("_No dimensions in catalog._")
    else:
        for d in sorted(entry.dimensions, key=lambda x: x.position):
            truncated = " (truncated)" if d.truncated else ""
            header = f"### `{d.id}` — position {d.position}{truncated}"
            lines.append(header)
            lines.append("")
            lines.append(f"Total codes: **{d.total_codes}**")
            if d.codes:
                preview = ", ".join(f"`{c.id}`" for c in d.codes[:12])
                if len(d.codes) > 12:
                    preview += f", …(+{len(d.codes) - 12})"
                lines.append(f"Codes: {preview}")
            lines.append("")

    if entry.common_queries:
        lines.append("## Common queries")
        lines.append("")
        for q in entry.common_queries:
            params = ", ".join(f"{k}={v}" for k, v in q.params.items())
            lines.append(f"- **{q.label}** → key `{q.key}` ({params or 'no params'})")
        lines.append("")

    return "\n".join(lines)


def render_category(cat: Catalog, category: str, *, language: str = "en") -> str:
    items = cat.list_dataflows(category=category, include_non_final=False)
    lines = [f"# Category `{category}`", ""]
    if not items:
        lines.append("_No dataflows in this category._")
        return "\n".join(lines)
    lines.append(f"**{len(items)} dataflows**")
    lines.append("")
    lines.append("| Agency | ID | Name | Dimensions |")
    lines.append("|---|---|---|---|")
    for e in items:
        name = e.names.get(language) or e.names.get("en") or e.id
        dims = ", ".join(f"`{d.id}`" for d in e.dimensions) or "—"
        lines.append(f"| `{e.agency}` | `{e.id}` | {name} | {dims} |")
    return "\n".join(lines)


__all__ = [
    "render_catalog_summary",
    "render_category",
    "render_dataflow",
]
