"""Tests for the enriched catalogue loader and search."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from nbb_mcp.catalog import Catalog, SearchResult
from nbb_mcp.models.catalog import (
    CatalogDimension,
    CodeRef,
    EnrichedDataflow,
    TimeCoverage,
)
from nbb_mcp.models.errors import NBBCatalogError


def _make_entry(
    agency: str,
    id: str,
    category: str,
    *,
    names: dict[str, str] | None = None,
    is_final: bool = True,
    dimensions: list[CatalogDimension] | None = None,
) -> EnrichedDataflow:
    return EnrichedDataflow(
        agency=agency,
        id=id,
        version="1.0",
        category=category,
        is_final=is_final,
        names=names or {"en": id},
        frequencies_available=[],
        default_frequency=None,
        time_coverage=TimeCoverage(),
        dimensions=dimensions or [],
        primary_measure="OBS_VALUE",
        key_template=None,
        common_queries=[],
        etag=None,
        fetched_at=None,
        schema_version=1,
    )


@pytest.fixture
def fiche_dir(tmp_path: Path) -> Path:
    """Build a tmp catalog dir with 4 hand-crafted fiches for fast/unit tests."""
    entries = [
        _make_entry(
            "BE2",
            "DF_EXR",
            "exchange_interest_rates",
            names={
                "en": "Reference exchange rates of the euro in national currency units",
                "fr": "Taux de change de référence de l'euro",
                "nl": "Referentiewisselkoersen van de euro",
                "de": "Referenzkurse des Euro",
            },
            dimensions=[
                CatalogDimension(
                    id="FREQ",
                    position=0,
                    name="Frequency",
                    codes=[CodeRef(id="D", name="Daily"), CodeRef(id="M", name="Monthly")],
                ),
                CatalogDimension(
                    id="EXR_CURRENCY",
                    position=1,
                    name="Currency",
                    codes=[CodeRef(id="USD", name="US Dollar"), CodeRef(id="GBP", name="British Pound")],
                ),
            ],
        ),
        _make_entry(
            "BE2",
            "DF_HICP_2025",
            "prices",
            names={
                "en": "Inflation and harmonised consumer price index (HICP)",
                "fr": "Inflation et indice des prix à la consommation harmonisé",
            },
        ),
        _make_entry(
            "BE2",
            "DF_QNA_DISS",
            "national_accounts",
            names={"en": "GDP according to the 3 approaches — quarterly and annual data"},
        ),
        _make_entry(
            "IMF",
            "CPI",
            "imf_sdds",
            names={"en": "Consumer Price Index"},
            is_final=False,
        ),
    ]
    for e in entries:
        (tmp_path / f"{e.agency}_{e.id}.json").write_text(e.model_dump_json(indent=2))

    index = {
        "built_at": "2026-04-12T00:00:00Z",
        "dataflow_count": len(entries),
        "schema_version": 1,
        "categories": {"exchange_interest_rates": 1, "prices": 1, "national_accounts": 1, "imf_sdds": 1},
        "flows": [{"agency": e.agency, "id": e.id, "category": e.category} for e in entries],
    }
    (tmp_path / "_index.json").write_text(json.dumps(index, indent=2))
    return tmp_path


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def test_catalog_load_reads_all_fiches(fiche_dir):
    cat = Catalog.load(fiche_dir)
    assert len(cat) == 4
    assert {(e.agency, e.id) for e in cat.entries} == {
        ("BE2", "DF_EXR"),
        ("BE2", "DF_HICP_2025"),
        ("BE2", "DF_QNA_DISS"),
        ("IMF", "CPI"),
    }


def test_catalog_load_ignores_index_and_underscore_files(fiche_dir):
    (fiche_dir / "_build_errors.json").write_text("[]")
    (fiche_dir / "_notes.json").write_text("{}")
    cat = Catalog.load(fiche_dir)
    assert len(cat) == 4


def test_catalog_load_empty_dir_raises(tmp_path):
    with pytest.raises(NBBCatalogError) as exc:
        Catalog.load(tmp_path)
    assert exc.value.code == "EMPTY_CATALOG"


def test_catalog_load_missing_dir_raises(tmp_path):
    with pytest.raises(NBBCatalogError) as exc:
        Catalog.load(tmp_path / "nope")
    assert exc.value.code == "MISSING_CATALOG_DIR"


def test_catalog_load_invalid_fiche_raises(tmp_path):
    (tmp_path / "BE2_BAD.json").write_text("{not json")
    with pytest.raises(NBBCatalogError) as exc:
        Catalog.load(tmp_path)
    assert exc.value.code == "INVALID_FICHE"


# ---------------------------------------------------------------------------
# Summaries
# ---------------------------------------------------------------------------


def test_catalog_categories_counts(fiche_dir):
    cat = Catalog.load(fiche_dir)
    cats = cat.categories()
    assert cats["exchange_interest_rates"] == 1
    assert cats["prices"] == 1
    assert cats["national_accounts"] == 1
    assert cats["imf_sdds"] == 1


def test_catalog_agencies_counts(fiche_dir):
    cat = Catalog.load(fiche_dir)
    assert cat.agencies() == {"BE2": 3, "IMF": 1}


def test_catalog_index_metadata(fiche_dir):
    cat = Catalog.load(fiche_dir)
    meta = cat.index_metadata(fiche_dir)
    assert meta is not None
    assert meta["dataflow_count"] == 4
    assert "schema_version" in meta


# ---------------------------------------------------------------------------
# Lookup / list
# ---------------------------------------------------------------------------


def test_catalog_get_by_key(fiche_dir):
    cat = Catalog.load(fiche_dir)
    entry = cat.get("BE2", "DF_EXR")
    assert entry.category == "exchange_interest_rates"
    assert len(entry.dimensions) == 2
    assert "Taux de change" in entry.names["fr"]


def test_catalog_get_unknown_raises(fiche_dir):
    cat = Catalog.load(fiche_dir)
    with pytest.raises(NBBCatalogError) as exc:
        cat.get("BE2", "DF_NOPE")
    assert exc.value.code == "UNKNOWN_FLOW"


def test_catalog_list_by_category(fiche_dir):
    cat = Catalog.load(fiche_dir)
    items = cat.list_dataflows(category="exchange_interest_rates")
    assert len(items) == 1
    assert items[0].id == "DF_EXR"


def test_catalog_list_by_agency(fiche_dir):
    cat = Catalog.load(fiche_dir)
    be2 = cat.list_dataflows(agency="BE2")
    assert len(be2) == 3  # IMF/CPI is non-final and filtered out by default
    assert all(e.agency == "BE2" for e in be2)


def test_catalog_list_hides_non_final_by_default(fiche_dir):
    cat = Catalog.load(fiche_dir)
    assert all(e.is_final for e in cat.list_dataflows())


def test_catalog_list_include_non_final(fiche_dir):
    cat = Catalog.load(fiche_dir)
    all_items = cat.list_dataflows(include_non_final=True)
    assert any(not e.is_final for e in all_items)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def test_catalog_search_english_exchange(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("exchange rate", limit=3)
    assert results
    assert any(r.entry.id == "DF_EXR" for r in results[:2])


def test_catalog_search_french(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("taux de change", limit=3)
    assert any(r.entry.id == "DF_EXR" for r in results)


def test_catalog_search_by_id(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("HICP", limit=3)
    assert results[0].entry.id == "DF_HICP_2025"


def test_catalog_search_empty_query_returns_empty(fiche_dir):
    cat = Catalog.load(fiche_dir)
    assert cat.search("") == []
    assert cat.search("x") == []


def test_catalog_search_with_category_filter(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("index", category="prices")
    assert results
    assert all(r.entry.category == "prices" for r in results)


def test_catalog_search_excludes_non_final_by_default(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("consumer", limit=5)
    assert all(r.entry.is_final for r in results)


def test_catalog_search_can_include_non_final(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("consumer", include_non_final=True, limit=5)
    assert any(not r.entry.is_final for r in results)


def test_catalog_search_returns_search_result_instances(fiche_dir):
    cat = Catalog.load(fiche_dir)
    results = cat.search("exchange")
    for r in results:
        assert isinstance(r, SearchResult)
        assert 0 <= r.score <= 100


# ---------------------------------------------------------------------------
# Performance — must stay well under 10 ms on 221 entries.
# ---------------------------------------------------------------------------


def test_catalog_search_is_fast_on_realistic_size(tmp_path):
    """Build a synthetic 250-entry catalog and assert search < 20 ms (margin)."""
    for i in range(250):
        e = _make_entry(
            "BE2",
            f"DF_FAKE_{i:03d}",
            "other",
            names={"en": f"Fake dataflow {i} with some words", "fr": f"Flux fictif numéro {i}"},
        )
        (tmp_path / f"{e.agency}_{e.id}.json").write_text(e.model_dump_json())

    cat = Catalog.load(tmp_path)
    assert len(cat) == 250

    start = time.perf_counter()
    for _ in range(20):
        cat.search("fake dataflow 42", limit=10)
    elapsed_ms_per_call = (time.perf_counter() - start) * 1000 / 20
    assert elapsed_ms_per_call < 20, f"search too slow: {elapsed_ms_per_call:.2f} ms/call"
