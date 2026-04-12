"""Translate ``{dimension: value}`` filter dicts into SDMX REST keys.

The NSI v8 data endpoint expects a positional key such as ``D.USD`` (two
dimensions, ``FREQ=D`` and ``EXR_CURRENCY=USD``). Missing dimensions are
wildcarded with an empty string (``D.`` for ``FREQ=D, EXR_CURRENCY=*``). A fully
wildcarded key is rewritten as ``"all"``.

Multi-value filters use the ``+`` separator (``D+M.USD``).

All validation errors are raised as :class:`NBBValidationError` with structured
details that the LLM can act on (valid dimensions, valid codes, key template).
"""

from __future__ import annotations

from .models.catalog import EnrichedDataflow
from .models.errors import NBBValidationError


def _valid_dim_ids(entry: EnrichedDataflow) -> list[str]:
    return [d.id for d in sorted(entry.dimensions, key=lambda x: x.position)]


def build_sdmx_key(
    entry: EnrichedDataflow,
    filters: dict[str, str | list[str]] | None = None,
) -> str:
    """Build an SDMX REST key from a filters dict, using the catalog DSD."""
    if not filters:
        return "all"
    if not entry.dimensions:
        raise NBBValidationError(
            f"Dataflow {entry.agency}/{entry.id} has no dimensions in the catalog — "
            "cannot build a filtered key. Pass 'key=\"all\"' instead.",
            code="NO_DIMENSIONS",
            details={"agency": entry.agency, "id": entry.id},
        )

    dim_by_id = {d.id: d for d in entry.dimensions}
    valid_ids = _valid_dim_ids(entry)

    unknown = [k for k in filters if k not in dim_by_id]
    if unknown:
        raise NBBValidationError(
            f"Unknown dimension(s) for {entry.agency}/{entry.id}: {sorted(unknown)}",
            code="UNKNOWN_DIMENSION",
            details={
                "unknown_dimensions": sorted(unknown),
                "valid_dimensions": valid_ids,
                "key_template": entry.key_template,
            },
        )

    for dim_id, raw in filters.items():
        dim = dim_by_id[dim_id]
        if not dim.codes:
            continue
        values = [raw] if isinstance(raw, str) else list(raw)
        valid_codes = {c.id for c in dim.codes}
        bad = [v for v in values if v not in valid_codes]
        if bad:
            sample = sorted(valid_codes)[:20]
            raise NBBValidationError(
                f"Unknown code(s) {bad} for dimension '{dim_id}' on {entry.agency}/{entry.id}",
                code="UNKNOWN_CODE",
                details={
                    "dimension": dim_id,
                    "invalid_codes": bad,
                    "valid_codes_sample": sample,
                    "total_valid_codes": dim.total_codes,
                    "codelist_truncated_in_catalog": dim.truncated,
                },
            )

    parts: list[str] = []
    for d in sorted(entry.dimensions, key=lambda x: x.position):
        v = filters.get(d.id)
        if v is None or v == "":
            parts.append("")
            continue
        if isinstance(v, list):
            parts.append("+".join(v))
        else:
            parts.append(v)

    if all(p == "" for p in parts):
        return "all"
    return ".".join(parts)


__all__ = ["build_sdmx_key"]
