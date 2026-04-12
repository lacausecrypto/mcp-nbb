"""``nbb_search`` — fuzzy search over the local enriched catalogue (no API)."""

from __future__ import annotations

from typing import Any

from ..services import get_services


def _localised_name(names: dict[str, str], language: str) -> str:
    return names.get(language) or names.get("en") or next(iter(names.values()), "")


async def run(
    query: str,
    *,
    category: str | None = None,
    agency: str | None = None,
    language: str = "en",
    include_non_final: bool = False,
    limit: int = 10,
) -> dict[str, Any]:
    svc = get_services()
    if category == "all":
        category = None
    if agency == "all":
        agency = None

    results = svc.catalog.search(
        query,
        category=category,
        agency=agency,
        include_non_final=include_non_final,
        limit=limit,
    )
    return {
        "query": query,
        "count": len(results),
        "results": [
            {
                "agency": r.entry.agency,
                "id": r.entry.id,
                "version": r.entry.version,
                "score": round(r.score, 1),
                "category": r.entry.category,
                "name": _localised_name(r.entry.names, language),
                "dimensions": [
                    {
                        "id": d.id,
                        "position": d.position,
                        "codes_in_catalog": len(d.codes),
                        "total_codes": d.total_codes,
                        "truncated": d.truncated,
                    }
                    for d in r.entry.dimensions
                ],
                "key_template": r.entry.key_template,
                "default_frequency": r.entry.default_frequency,
                "common_queries": [
                    {"label": q.label, "key": q.key, "params": q.params}
                    for q in r.entry.common_queries[:3]
                ],
            }
            for r in results
        ],
    }
