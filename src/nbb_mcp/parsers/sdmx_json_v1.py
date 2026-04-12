"""Minimal parser for SDMX-JSON 1.0 structure responses.

Scope: what ``nbb_mcp`` actually consumes from ``/dataflow`` and ``/datastructure``
endpoints served by NSI Web Service v8 — not a full reference implementation.

Expected shape::

    {
      "data": {
        "dataflows": [...],
        "dataStructures": [...],
        "codelists": [...]
      },
      "meta": {...}
    }
"""

from __future__ import annotations

import re
from typing import Any

from ..models.errors import NBBParseError
from ..models.sdmx import Code, DataflowDetail, DataflowStub, DataStructure, Dimension

_URN_CODELIST_RE = re.compile(r"Codelist=([^:]+):([^(]+)\((\d[\d.]*)\)")


def _get(d: dict[str, Any], key: str) -> Any:
    try:
        return d[key]
    except KeyError as exc:
        raise NBBParseError(
            f"Missing key '{key}' in SDMX structure payload",
            code="MISSING_KEY",
            details={"available_keys": list(d.keys())},
        ) from exc


def _root_data(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the ``data`` section, raising a typed error on malformed payloads."""
    if not isinstance(payload, dict):
        raise NBBParseError(
            f"Expected dict at top level, got {type(payload).__name__}",
            code="INVALID_ROOT",
        )
    return _get(payload, "data")


def _codelist_ref(urn: str | None) -> tuple[str, str, str] | None:
    """Parse a codelist URN like ``urn:…Codelist=BE2:CL_FREQ(1.0)``."""
    if not urn:
        return None
    m = _URN_CODELIST_RE.search(urn)
    if not m:
        return None
    return (m.group(1), m.group(2), m.group(3))


def parse_dataflow_list(payload: dict[str, Any]) -> list[DataflowStub]:
    """Parse the response of ``/dataflow/all/all/latest``."""
    data = _root_data(payload)
    raw = data.get("dataflows") or []
    stubs: list[DataflowStub] = []
    for df in raw:
        stubs.append(
            DataflowStub(
                agency=df.get("agencyID", ""),
                id=df.get("id", ""),
                version=df.get("version", "1.0"),
                name=df.get("name"),
                names=df.get("names") or {},
                description=df.get("description"),
                is_final=bool(df.get("isFinal", True)),
            )
        )
    return stubs


def _index_codelists(data: dict[str, Any]) -> dict[tuple[str, str, str], list[Code]]:
    """Index codelists by ``(agency, id, version)`` for dimension resolution."""
    index: dict[tuple[str, str, str], list[Code]] = {}
    for cl in data.get("codelists") or []:
        key = (
            cl.get("agencyID", ""),
            cl.get("id", ""),
            cl.get("version", "1.0"),
        )
        codes: list[Code] = []
        for c in cl.get("codes") or []:
            codes.append(Code(id=c.get("id", ""), name=c.get("name")))
        index[key] = codes
    return index


def parse_dataflow_detail(payload: dict[str, Any]) -> DataflowDetail:
    """Parse ``/dataflow/{a}/{id}/{v}?references=all``.

    Expects the payload to contain the dataflow, its DSD, and referenced codelists.
    """
    data = _root_data(payload)
    dataflows = data.get("dataflows") or []
    if not dataflows:
        raise NBBParseError("No dataflow in detail response", code="EMPTY_DATAFLOW")
    df = dataflows[0]

    structures = data.get("dataStructures") or []
    structure: DataStructure | None = None
    if structures:
        dsd = structures[0]
        comps = dsd.get("dataStructureComponents") or {}
        dim_list = (comps.get("dimensionList") or {}).get("dimensions") or []

        codelist_index = _index_codelists(data)
        dimensions: list[Dimension] = []
        for d in dim_list:
            did = d.get("id", "")
            pos = int(d.get("position", 0))
            local = d.get("localRepresentation") or {}
            cl_key = _codelist_ref(local.get("enumeration"))
            codes = codelist_index.get(cl_key, []) if cl_key else []
            dimensions.append(Dimension(id=did, position=pos, name=did, codes=codes))

        time_dims = (comps.get("dimensionList") or {}).get("timeDimensions") or []
        time_dim_id = (time_dims[0].get("id") if time_dims else "TIME_PERIOD") or "TIME_PERIOD"

        structure = DataStructure(
            agency=dsd.get("agencyID", ""),
            id=dsd.get("id", ""),
            version=dsd.get("version", "1.0"),
            name=dsd.get("name"),
            dimensions=sorted(dimensions, key=lambda x: x.position),
            time_dimension=time_dim_id,
        )

    return DataflowDetail(
        agency=df.get("agencyID", ""),
        id=df.get("id", ""),
        version=df.get("version", "1.0"),
        name=df.get("name"),
        names=df.get("names") or {},
        description=df.get("description"),
        is_final=bool(df.get("isFinal", True)),
        structure=structure,
    )


__all__ = ["parse_dataflow_detail", "parse_dataflow_list"]
