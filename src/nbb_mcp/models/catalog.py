"""Models for the enriched local catalogue."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CodeRef(BaseModel):
    id: str
    name: str | None = None


class CatalogDimension(BaseModel):
    id: str
    position: int
    name: str | None = None
    codes: list[CodeRef] = Field(default_factory=list)
    total_codes: int = 0
    truncated: bool = False


class CommonQuery(BaseModel):
    label: str
    key: str
    params: dict[str, str | int] = Field(default_factory=dict)


class TimeCoverage(BaseModel):
    start: str | None = None
    end: str | None = None


class EnrichedDataflow(BaseModel):
    """A fully enriched dataflow entry bundled in the catalog/."""

    agency: str
    id: str
    version: str
    category: str
    is_final: bool = True
    names: dict[str, str] = Field(default_factory=dict)
    frequencies_available: list[str] = Field(default_factory=list)
    default_frequency: str | None = None
    time_coverage: TimeCoverage = Field(default_factory=TimeCoverage)
    dimensions: list[CatalogDimension] = Field(default_factory=list)
    primary_measure: str = "OBS_VALUE"
    key_template: str | None = None
    common_queries: list[CommonQuery] = Field(default_factory=list)
    etag: str | None = None
    fetched_at: str | None = None
    schema_version: int = 1
