"""Minimal parser for SDMX-JSON 2.0 data responses served by NSI v8.

Expected shape::

    {
      "meta": {...},
      "data": {
        "dataSets": [
          {
            "series": {
              "0:1": {
                 "observations": {"0": [1.6469, 0, 0], "1": [...]},
                 "attributes": [...]
              }
            }
          }
        ],
        "structures": [
          {
            "dimensions": {
              "series": [
                {"id": "FREQ", "keyPosition": 0, "values": [{"id": "D", "name": "Daily"}, ...]},
                {"id": "EXR_CURRENCY", "keyPosition": 1, "values": [...]}
              ],
              "observation": [
                {"id": "TIME_PERIOD", "values": [{"id": "2024-01-02"}, ...]}
              ]
            }
          }
        ]
      }
    }

Series keys like ``"0:1"`` are colon-separated positional indices into the ordered
``dimensions.series`` arrays. Observation keys like ``"0"`` are positional indices
into ``dimensions.observation[0].values``.
"""

from __future__ import annotations

from typing import Any

from ..models.errors import NBBParseError
from ..models.sdmx import DataMessage, Observation, Series


def _get(d: dict[str, Any], key: str, context: str) -> Any:
    try:
        return d[key]
    except KeyError as exc:
        raise NBBParseError(
            f"Missing key '{key}' in SDMX data payload ({context})",
            code="MISSING_KEY",
        ) from exc


def parse_data_message(payload: dict[str, Any], *, dataflow: str = "") -> DataMessage:
    """Parse an SDMX-JSON 2.0 data response into :class:`DataMessage`."""
    if not isinstance(payload, dict):
        raise NBBParseError(
            f"Expected dict at top level, got {type(payload).__name__}",
            code="INVALID_ROOT",
        )
    meta = payload.get("meta") or {}
    data = _get(payload, "data", "top")
    datasets = data.get("dataSets") or []
    structures = data.get("structures") or []

    if not datasets or not structures:
        return DataMessage(dataflow=dataflow, series=[], raw_meta=meta)

    dataset = datasets[0]
    structure = structures[0]
    dims = structure.get("dimensions") or {}
    series_dims: list[dict[str, Any]] = dims.get("series") or []
    obs_dims: list[dict[str, Any]] = dims.get("observation") or []

    # Order series dims by keyPosition (fallback to current order).
    series_dims = sorted(series_dims, key=lambda d: d.get("keyPosition", 0))
    series_dim_ids = [d.get("id", "") for d in series_dims]
    series_dim_values: list[list[dict[str, Any]]] = [d.get("values") or [] for d in series_dims]

    # Observation dimension — canonical case: a single TIME_PERIOD dim.
    if obs_dims:
        obs_dim = obs_dims[0]
        obs_values: list[dict[str, Any]] = obs_dim.get("values") or []
    else:
        obs_values = []

    series_out: list[Series] = []
    raw_series: dict[str, Any] = dataset.get("series") or {}

    for series_key, series_body in raw_series.items():
        # Resolve positional key "0:1" -> concrete dimension values.
        try:
            positions = [int(p) for p in series_key.split(":")] if series_key else []
        except ValueError as exc:
            raise NBBParseError(
                f"Invalid series positional key '{series_key}'",
                code="INVALID_SERIES_KEY",
            ) from exc

        if len(positions) != len(series_dim_ids):
            # Tolerate mismatch but warn via details — we slice to the shorter length.
            pairs = list(zip(series_dim_ids, positions, strict=False))
        else:
            pairs = list(zip(series_dim_ids, positions, strict=True))

        dim_map: dict[str, str] = {}
        key_parts: list[str] = []
        for (dim_id, idx), values in zip(pairs, series_dim_values, strict=False):
            if 0 <= idx < len(values):
                val_id = values[idx].get("id", "")
            else:
                val_id = ""
            dim_map[dim_id] = val_id
            key_parts.append(val_id)
        resolved_key = ".".join(key_parts)

        obs_list: list[Observation] = []
        raw_obs: dict[str, Any] = series_body.get("observations") or {}
        for obs_key, obs_arr in raw_obs.items():
            try:
                obs_idx = int(obs_key)
            except ValueError:
                continue
            period = ""
            if 0 <= obs_idx < len(obs_values):
                period = obs_values[obs_idx].get("id", "") or ""
            value: float | None = None
            status: str | None = None
            if isinstance(obs_arr, list) and obs_arr:
                raw_val = obs_arr[0]
                if raw_val is None:
                    value = None
                else:
                    try:
                        value = float(raw_val)
                    except (TypeError, ValueError):
                        value = None
            obs_list.append(Observation(period=period, value=value, status=status))

        # Ensure chronological order for downstream formatters.
        obs_list.sort(key=lambda o: o.period)

        series_out.append(Series(key=resolved_key, dimensions=dim_map, observations=obs_list))

    return DataMessage(dataflow=dataflow, series=series_out, raw_meta=meta)


__all__ = ["parse_data_message"]
