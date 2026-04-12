"""``nbb_describe`` — return the enriched fiche for a single dataflow.

By default the data comes from the bundled catalogue (0 API calls). Passing
``force_refresh=True`` re-fetches the DSD from the live API and returns the
fresh structure without touching the on-disk catalogue.
"""

from __future__ import annotations

from typing import Any

from ..parsers.sdmx_json_v1 import parse_dataflow_detail
from ..services import get_services


def _localised_name(names: dict[str, str], language: str) -> str:
    return names.get(language) or names.get("en") or next(iter(names.values()), "")


async def run(
    dataflow_id: str,
    *,
    agency: str = "BE2",
    language: str = "en",
    include_codes: bool = True,
    force_refresh: bool = False,
) -> dict[str, Any]:
    svc = get_services()
    entry = svc.catalog.get(agency, dataflow_id)

    if force_refresh:
        payload = await svc.client.get_dataflow(agency, dataflow_id, entry.version)
        detail = parse_dataflow_detail(payload)
        live = {
            "structure": {
                "dimensions": [
                    {
                        "id": d.id,
                        "position": d.position,
                        "codes": (
                            [{"id": c.id, "name": c.name} for c in d.codes]
                            if include_codes
                            else None
                        ),
                    }
                    for d in detail.structure.dimensions
                ]
                if detail.structure
                else [],
                "time_dimension": detail.structure.time_dimension if detail.structure else None,
                "primary_measure": detail.structure.primary_measure if detail.structure else None,
            }
        }
    else:
        live = None

    return {
        "agency": entry.agency,
        "id": entry.id,
        "version": entry.version,
        "category": entry.category,
        "name": _localised_name(entry.names, language),
        "names": entry.names,
        "primary_measure": entry.primary_measure,
        "default_frequency": entry.default_frequency,
        "frequencies_available": entry.frequencies_available,
        "key_template": entry.key_template,
        "dimensions": [
            {
                "id": d.id,
                "position": d.position,
                "name": d.name,
                "total_codes": d.total_codes,
                "truncated": d.truncated,
                "codes": (
                    [{"id": c.id, "name": c.name} for c in d.codes]
                    if include_codes
                    else None
                ),
            }
            for d in entry.dimensions
        ],
        "common_queries": [
            {"label": q.label, "key": q.key, "params": q.params}
            for q in entry.common_queries
        ],
        "live_refresh": live,
    }
