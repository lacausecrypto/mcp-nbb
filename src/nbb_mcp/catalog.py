"""Loader for the bundled enriched catalogue + fuzzy search.

The catalogue lives under ``src/nbb_mcp/data/catalog/`` and contains one JSON
fiche per dataflow (:class:`EnrichedDataflow`) plus an ``_index.json`` summary.
The :class:`Catalog` class loads all fiches into memory once at construction and
serves them synchronously from then on — no I/O on the hot path.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rapidfuzz import fuzz, process

from .models.catalog import EnrichedDataflow
from .models.errors import NBBCatalogError


def _combined_scorer(query: str, choice: str, **_kwargs) -> float:
    """Hybrid scorer for long multilingual haystacks.

    ``token_set_ratio`` is strong on multi-word queries against long documents but
    weak on short ID-only queries like ``"HICP"``. ``partial_ratio`` handles those
    well but over-triggers on incidental character matches (e.g. ``"taux de
    change"`` wrongly favouring ``DF_UNEMPLOY_RATE``). We take the max of both,
    attenuating partial_ratio slightly so token matches dominate when present.
    """
    tsr = fuzz.token_set_ratio(query, choice)
    pr = fuzz.partial_ratio(query, choice) * 0.85
    return max(tsr, pr)

_CATALOG_DIR = Path(__file__).resolve().parent / "data" / "catalog"
_INDEX_FILENAME = "_index.json"
_PRIVATE_PREFIX = "_"


@dataclass(frozen=True)
class SearchResult:
    entry: EnrichedDataflow
    score: float


def _is_fiche_file(p: Path) -> bool:
    return p.is_file() and p.suffix == ".json" and not p.name.startswith(_PRIVATE_PREFIX)


def _build_haystack(entry: EnrichedDataflow) -> str:
    parts: list[str] = [entry.id, entry.category, entry.agency]
    for v in entry.names.values():
        if v:
            parts.append(v)
    return " | ".join(parts)


class Catalog:
    """In-memory view over the bundled enriched catalogue."""

    def __init__(self, entries: list[EnrichedDataflow]) -> None:
        if not entries:
            raise NBBCatalogError(
                "Empty catalogue (no fiches loaded)",
                code="EMPTY_CATALOG",
            )
        self._entries: list[EnrichedDataflow] = list(entries)
        self._by_key: dict[tuple[str, str], EnrichedDataflow] = {
            (e.agency, e.id): e for e in self._entries
        }
        self._by_category: dict[str, list[EnrichedDataflow]] = defaultdict(list)
        self._by_agency: dict[str, list[EnrichedDataflow]] = defaultdict(list)
        for e in self._entries:
            self._by_category[e.category].append(e)
            self._by_agency[e.agency].append(e)
        # Pre-build haystacks once. rapidfuzz.process.extract operates on a
        # mapping so we can return structured keys.
        self._haystacks: dict[tuple[str, str], str] = {
            (e.agency, e.id): _build_haystack(e) for e in self._entries
        }

    # ------------------------------------------------------------------ #
    # Loading
    # ------------------------------------------------------------------ #

    @classmethod
    def load(cls, directory: Path | None = None) -> Catalog:
        """Load every fiche under ``directory`` (default: bundled path)."""
        base = directory or _CATALOG_DIR
        if not base.is_dir():
            raise NBBCatalogError(
                f"Catalogue directory not found: {base}",
                code="MISSING_CATALOG_DIR",
                details={"path": str(base)},
            )
        fiches: list[EnrichedDataflow] = []
        for f in sorted(base.iterdir()):
            if not _is_fiche_file(f):
                continue
            try:
                data = json.loads(f.read_text())
                fiches.append(EnrichedDataflow.model_validate(data))
            except (OSError, ValueError) as exc:
                raise NBBCatalogError(
                    f"Failed to load fiche {f.name}: {exc}",
                    code="INVALID_FICHE",
                    details={"file": str(f)},
                ) from exc
        return cls(fiches)

    # ------------------------------------------------------------------ #
    # Summary accessors
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def entries(self) -> list[EnrichedDataflow]:
        return list(self._entries)

    def categories(self) -> dict[str, int]:
        return {cat: len(items) for cat, items in sorted(self._by_category.items())}

    def agencies(self) -> dict[str, int]:
        return {agency: len(items) for agency, items in sorted(self._by_agency.items())}

    def index_metadata(self, directory: Path | None = None) -> dict | None:
        """Return the raw ``_index.json`` content if present (summary for nbb_status)."""
        base = directory or _CATALOG_DIR
        index = base / _INDEX_FILENAME
        if not index.is_file():
            return None
        try:
            return json.loads(index.read_text())
        except (OSError, ValueError):
            return None

    # ------------------------------------------------------------------ #
    # Lookup
    # ------------------------------------------------------------------ #

    def get(self, agency: str, dataflow_id: str) -> EnrichedDataflow:
        try:
            return self._by_key[(agency, dataflow_id)]
        except KeyError as exc:
            raise NBBCatalogError(
                f"Unknown dataflow '{agency}/{dataflow_id}'",
                code="UNKNOWN_FLOW",
                details={"agency": agency, "id": dataflow_id},
            ) from exc

    def list_dataflows(
        self,
        *,
        category: str | None = None,
        agency: str | None = None,
        include_non_final: bool = False,
    ) -> list[EnrichedDataflow]:
        out: Iterable[EnrichedDataflow]
        if category and category != "all":
            out = self._by_category.get(category, [])
        elif agency and agency != "all":
            out = self._by_agency.get(agency, [])
        else:
            out = self._entries

        result = [e for e in out if include_non_final or e.is_final]
        if category and category != "all" and agency and agency != "all":
            result = [e for e in result if e.agency == agency]
        return sorted(result, key=lambda e: (e.agency, e.id))

    # ------------------------------------------------------------------ #
    # Search
    # ------------------------------------------------------------------ #

    def search(
        self,
        query: str,
        *,
        category: str | None = None,
        agency: str | None = None,
        include_non_final: bool = False,
        limit: int = 10,
    ) -> list[SearchResult]:
        """Fuzzy search across ids, categories, and multilingual names."""
        if not query or len(query.strip()) < 2:
            return []

        # Narrow the search space *before* calling rapidfuzz — much faster on
        # filtered subsets, and category/agency filters become precise.
        candidates = self.list_dataflows(
            category=category,
            agency=agency,
            include_non_final=include_non_final,
        )
        if not candidates:
            return []

        haystacks: dict[tuple[str, str], str] = {
            (e.agency, e.id): self._haystacks[(e.agency, e.id)] for e in candidates
        }
        raw = process.extract(
            query,
            haystacks,
            scorer=_combined_scorer,
            limit=limit,
        )
        out: list[SearchResult] = []
        for _match_text, score, key in raw:
            entry = self._by_key.get(key)
            if entry is None:
                continue
            out.append(SearchResult(entry=entry, score=float(score)))
        return out


__all__ = ["Catalog", "SearchResult"]
