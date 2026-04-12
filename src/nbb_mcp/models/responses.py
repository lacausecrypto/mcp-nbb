"""LLM-friendly response envelopes returned by the data tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ObservationPoint(BaseModel):
    period: str
    value: float | None = None


class SeriesStats(BaseModel):
    """Aggregate statistics for a single SDMX series."""

    key: str
    dimensions: dict[str, str] = Field(default_factory=dict)
    observation_count: int = 0
    period_start: str | None = None
    period_end: str | None = None
    first: ObservationPoint | None = None
    latest: ObservationPoint | None = None
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    change_absolute: float | None = None
    change_pct: float | None = None


class FormattedResponse(BaseModel):
    """Top-level payload returned by ``nbb_query`` and ``nbb_quick``."""

    summary: dict[str, Any] = Field(default_factory=dict)
    series: list[SeriesStats] = Field(default_factory=list)
    data: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    truncated: bool = False
    next_page_hint: dict[str, Any] | None = None


__all__ = ["FormattedResponse", "ObservationPoint", "SeriesStats"]
