"""Smoke tests against the real bundled catalogue.

These assertions guard the shipped ``src/nbb_mcp/data/catalog/`` directory:
they fail loudly if a rebuild drops critical flows, loses multilingual names,
or balloons the on-disk footprint.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nbb_mcp.catalog import Catalog
from nbb_mcp.models.errors import NBBCatalogError

BUNDLED_DIR = Path(__file__).resolve().parent.parent / "src" / "nbb_mcp" / "data" / "catalog"

pytestmark = pytest.mark.skipif(
    not BUNDLED_DIR.is_dir() or not any(BUNDLED_DIR.glob("*.json")),
    reason="Bundled catalogue not built (run `python -m nbb_mcp.scripts.build_catalog`).",
)


@pytest.fixture(scope="module")
def bundled_catalog() -> Catalog:
    try:
        return Catalog.load(BUNDLED_DIR)
    except NBBCatalogError as exc:  # pragma: no cover
        pytest.skip(f"Catalogue load failed: {exc}")


def test_bundled_catalog_has_221_fiches(bundled_catalog):
    assert len(bundled_catalog) == 221


def test_bundled_catalog_has_both_agencies(bundled_catalog):
    agencies = bundled_catalog.agencies()
    assert agencies == {"BE2": 194, "IMF": 27}


def test_bundled_catalog_has_all_14_categories(bundled_catalog):
    categories = bundled_catalog.categories()
    expected = {
        "balance_of_payments",
        "corporate_accounts",
        "exchange_interest_rates",
        "financial_accounts",
        "financial_institutions",
        "foreign_trade",
        "imf_sdds",
        "labour_population",
        "national_accounts",
        "prices",
        "public_finances",
        "regional",
        "surveys",
    }
    assert expected.issubset(categories.keys())


def test_bundled_catalog_df_exr_is_complete(bundled_catalog):
    entry = bundled_catalog.get("BE2", "DF_EXR")
    assert entry.category == "exchange_interest_rates"
    assert entry.key_template == "{FREQ}.{EXR_CURRENCY}"
    assert {d.id for d in entry.dimensions} >= {"FREQ", "EXR_CURRENCY"}
    freq = next(d for d in entry.dimensions if d.id == "FREQ")
    assert "D" in {c.id for c in freq.codes}
    currency = next(d for d in entry.dimensions if d.id == "EXR_CURRENCY")
    assert "USD" in {c.id for c in currency.codes}
    assert len(entry.common_queries) >= 3
    assert any("USD" in q.label or "USD" in q.key for q in entry.common_queries)


def test_bundled_catalog_multilang_names_present(bundled_catalog):
    """Reference flows should have at least two localised names."""
    for agency, fid in [("BE2", "DF_EXR"), ("BE2", "DF_HICP_2025"), ("BE2", "DF_QNA_DISS")]:
        entry = bundled_catalog.get(agency, fid)
        assert "en" in entry.names, f"{agency}/{fid} missing en name"
        localised = [lang for lang in ("fr", "nl") if entry.names.get(lang)]
        assert localised, f"{agency}/{fid} has no fr/nl localisation"


def test_bundled_catalog_generic_fiches_have_common_queries(bundled_catalog):
    """Every fiche must ship at least the 2 generic common_queries."""
    missing = [
        (e.agency, e.id) for e in bundled_catalog.entries if len(e.common_queries) < 2
    ]
    assert not missing, f"Fiches missing common_queries: {missing[:5]}"


def test_bundled_catalog_search_usage_examples(bundled_catalog):
    """Smoke checks for the main search journeys Phase 3 will rely on."""
    # English: exchange rate → DF_EXR must be in the top 5.
    results = bundled_catalog.search("exchange rate of the euro", limit=5)
    ids = [r.entry.id for r in results]
    assert "DF_EXR" in ids

    # French: inflation → HICP flows must dominate.
    results = bundled_catalog.search("inflation indice prix", limit=5)
    assert any(r.entry.id.startswith("DF_HICP") or r.entry.id.startswith("DF_NICP") for r in results)

    # Topic: GDP → DF_QNA_DISS near the top.
    results = bundled_catalog.search("GDP quarterly", limit=5)
    assert any(r.entry.id == "DF_QNA_DISS" for r in results)


def test_bundled_catalog_oversized_codelists_are_truncated(bundled_catalog):
    """Ensure no fiche ships a codelist with more than the cap."""
    from nbb_mcp.scripts.build_catalog import MAX_CODES_PER_DIMENSION

    for entry in bundled_catalog.entries:
        for dim in entry.dimensions:
            if dim.truncated:
                assert dim.total_codes > MAX_CODES_PER_DIMENSION
                assert len(dim.codes) == MAX_CODES_PER_DIMENSION
            else:
                assert dim.total_codes == len(dim.codes)


def test_bundled_catalog_footprint_under_20mb(bundled_catalog):
    total = sum(f.stat().st_size for f in BUNDLED_DIR.glob("*.json"))
    size_mb = total / (1024 * 1024)
    assert size_mb < 20, f"catalogue is {size_mb:.1f} MB (> 20 MB cap)"


def test_bundled_catalog_index_metadata(bundled_catalog):
    meta = bundled_catalog.index_metadata(BUNDLED_DIR)
    assert meta is not None
    assert meta["dataflow_count"] == 221
    assert "categories" in meta
