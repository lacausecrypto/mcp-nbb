"""Domain models for parsed SDMX structures and data."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class Code(BaseModel):
    """A single code within a codelist."""

    id: str
    name: str | None = None


class Dimension(BaseModel):
    """A dataflow dimension with its position and (optionally) codes."""

    id: str
    position: int
    name: str | None = None
    codes: list[Code] = Field(default_factory=list)


class Attribute(BaseModel):
    id: str
    name: str | None = None


class DataStructure(BaseModel):
    """Data Structure Definition (DSD) summary."""

    agency: str
    id: str
    version: str
    name: str | None = None
    dimensions: list[Dimension] = Field(default_factory=list)
    attributes: list[Attribute] = Field(default_factory=list)
    time_dimension: str = "TIME_PERIOD"
    primary_measure: str = "OBS_VALUE"


class DataflowStub(BaseModel):
    """Lightweight dataflow reference (as returned by dataflow/all)."""

    agency: str
    id: str
    version: str
    name: str | None = None
    names: dict[str, str] = Field(default_factory=dict)
    description: str | None = None
    is_final: bool = True


class DataflowDetail(BaseModel):
    """Detailed dataflow metadata including DSD."""

    agency: str
    id: str
    version: str
    name: str | None = None
    names: dict[str, str] = Field(default_factory=dict)
    description: str | None = None
    is_final: bool = True
    structure: DataStructure | None = None


class Observation(BaseModel):
    period: str
    value: float | None = None
    status: str | None = None


class Series(BaseModel):
    key: str
    dimensions: dict[str, str] = Field(default_factory=dict)
    observations: list[Observation] = Field(default_factory=list)


class DataMessage(BaseModel):
    """Parsed SDMX data response."""

    dataflow: str
    series: list[Series] = Field(default_factory=list)
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    raw_meta: dict[str, Any] = Field(default_factory=dict)

    @property
    def total_observations(self) -> int:
        return sum(len(s.observations) for s in self.series)
