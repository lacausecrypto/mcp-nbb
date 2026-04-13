"""Build the enriched dataflow catalogue bundled with nbb-mcp.

For each of the 221 dataflows exposed by ``https://nsidisseminate-stat.nbb.be/rest``
this script fetches the full DSD + codelists, classifies the flow into one of the
14 categories, and writes a single JSON fiche to ``src/nbb_mcp/data/catalog/``.

Features:
    * Multi-language names: fetches the dataflow listing in en/fr/nl/de and merges
      the localised names into each fiche.
    * Bounded parallelism: at most N concurrent DSD requests (default 5), subject
      to the client's global aiolimiter.
    * Idempotent: existing fiches are skipped unless ``--force`` is passed.
    * Partial: on failure of a single flow, the others still complete and a
      ``_build_errors.json`` report is written.

Usage::

    python -m nbb_mcp.scripts.build_catalog            # full build
    python -m nbb_mcp.scripts.build_catalog --limit 5  # smoke test
    python -m nbb_mcp.scripts.build_catalog --force    # refetch everything
    python -m nbb_mcp.scripts.build_catalog --only BE2/DF_EXR,BE2/DF_HICP_2025
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..client import NBBClient, nbb_client
from ..config import Settings, get_settings
from ..logging import configure_logging, get_logger
from ..models.catalog import (
    CatalogDimension,
    CodeRef,
    CommonQuery,
    EnrichedDataflow,
    TimeCoverage,
)
from ..models.errors import NBBError
from ..models.sdmx import DataflowDetail, DataflowStub
from ..parsers.sdmx_json_v1 import parse_dataflow_detail, parse_dataflow_list

log = get_logger("nbb_mcp.build_catalog")

SUPPORTED_LANGUAGES = ("en", "fr", "nl", "de")
DEFAULT_CONCURRENCY = 5
SCHEMA_VERSION = 1
MAX_CODES_PER_DIMENSION = 200

# --------------------------------------------------------------------------- #
# Classification — mirrors DATAFLOWS_CATALOG.md (14 categories).
# --------------------------------------------------------------------------- #


def classify(stub: DataflowStub) -> str:
    agency, did = stub.agency, stub.id

    if agency == "IMF":
        return "imf_sdds"

    if agency != "BE2":
        return "other"

    if did in {
        "DF_EXR",
        "DF_IRESCB",
        "DF_IROLOBE2",
        "DF_IROLOYLD",
        "DF_IRTRCERT",
        "DF_MIR",
        "DF_MIRCCO",
    }:
        return "exchange_interest_rates"

    if did in {
        "DF_HICP",
        "DF_HICP_2025",
        "DF_NICP",
        "DF_NICP_2025",
        "DF_HISTO_NICP",
        "DF_PPI",
        "DF_INDPROD",
        "DF_AGREED_WAGES",
    }:
        return "prices"

    if did in {
        "DF_CONSN",
        "DF_BUSSURVM",
        "DF_BUSSURVQ",
        "DF_BUSSURVH2",
        "DF_AFCSURV",
        "DF_AFCSURV_CREDIT",
        "DF_BLS",
        "DF_BLSCCO",
        "DF_CONSTRUCTION",
        "DF_AMPORTS",
    }:
        return "surveys"

    if did in {
        "DF_EMPLOY_DISS",
        "DF_EMPLOYDETAIL_DISS",
        "DF_UNEMPLOYMENT",
        "DF_UNEMPLOY_RATE",
        "DF_POPULA",
    }:
        return "labour_population"

    if (
        did
        in {
            "DF_BOPBPM6",
            "DF_IIP",
            "DF_EXTDEBT",
            "DF_RESERVBPM6",
            "DF_ITSS",
        }
        or did.startswith("DF_FDI")
        or did.startswith("DF_STEC")
    ):
        return "balance_of_payments"

    if did.startswith("DF_FA") or did.startswith("DF_FINACC"):
        return "financial_accounts"

    if (
        did.startswith("DF_BSI")
        or did.startswith("DF_CREDINS")
        or did.startswith("DF_CRED")
        or did
        in {
            "DF_CCR",
            "DF_CICR",
            "DF_CRECONSURV",
            "DF_MORTGAGE",
            "DF_MONAGG",
            "DF_FVC",
            "DF_NMUCI",
            "DF_UCIDEV",
            "DF_MCUCIIC",
            "DF_PAYSTAT",
            "DF_ISSHARES",
        }
    ):
        return "financial_institutions"

    if did.startswith("DF_CBANACC") or did.startswith("DF_CBRATIOS") or did == "DF_CBSOCBAL":
        return "corporate_accounts"

    if did.startswith("DF_REG"):
        return "regional"

    if did.startswith("DF_EXTT") or did.startswith("DF_EXTERNAL_TRADE") or did == "DF_IEGSGEO_DISS":
        return "foreign_trade"

    if (
        did.startswith("DF_NFGOV")
        or did.startswith("DF_CGD")
        or did.startswith("DF_FINGOV")
        or did.startswith("DF_TREAS")
        or did in {"DF_AMOLO", "DF_FISEC", "DF_SHS_HOLDER"}
    ):
        return "public_finances"

    if (
        did.startswith("DF_QNA")
        or did.startswith("DF_NA")
        or did.startswith("DF_SUT")
        or did.startswith("DF_CAPSTOCK")
        or did.startswith("DF_PENS")
        or did == "DF_SATELLITE_DISS"
    ):
        return "national_accounts"

    return "other"


# --------------------------------------------------------------------------- #
# Common query mappings — specific flows used by nbb_quick (Phase 3).
# Keyed by (agency, dataflow_id). All other flows fall back to GENERIC_QUERIES.
# --------------------------------------------------------------------------- #

GENERIC_QUERIES: list[CommonQuery] = [
    CommonQuery(label="Latest value", key="all", params={"lastNObservations": 1}),
    CommonQuery(label="Last 12 observations", key="all", params={"lastNObservations": 12}),
]

SPECIFIC_QUERIES: dict[tuple[str, str], list[CommonQuery]] = {
    ("BE2", "DF_EXR"): [
        CommonQuery(
            label="EUR/USD daily, last 30 days", key="D.USD", params={"lastNObservations": 30}
        ),
        CommonQuery(
            label="EUR/GBP monthly 2024",
            key="M.GBP",
            params={"startPeriod": "2024-01", "endPeriod": "2024-12"},
        ),
        CommonQuery(label="EUR/JPY daily last week", key="D.JPY", params={"lastNObservations": 7}),
    ],
}


# --------------------------------------------------------------------------- #
# Fiche builders.
# --------------------------------------------------------------------------- #


def _detect_default_frequency(freq_codes: list[str]) -> str | None:
    for preferred in ("D", "M", "Q", "A"):
        if preferred in freq_codes:
            return preferred
    return freq_codes[0] if freq_codes else None


def build_fiche(
    stub: DataflowStub,
    detail: DataflowDetail,
    *,
    category: str,
    multilang_names: dict[str, str],
) -> EnrichedDataflow:
    dimensions: list[CatalogDimension] = []
    freq_codes: list[str] = []
    primary_measure = "OBS_VALUE"

    if detail.structure:
        primary_measure = detail.structure.primary_measure
        for d in sorted(detail.structure.dimensions, key=lambda x: x.position):
            total = len(d.codes)
            truncated = total > MAX_CODES_PER_DIMENSION
            raw_codes = d.codes[:MAX_CODES_PER_DIMENSION] if truncated else d.codes
            dimensions.append(
                CatalogDimension(
                    id=d.id,
                    position=d.position,
                    name=d.name,
                    codes=[CodeRef(id=c.id, name=c.name) for c in raw_codes],
                    total_codes=total,
                    truncated=truncated,
                )
            )
            if d.id == "FREQ":
                freq_codes = [c.id for c in d.codes]

    key_template = ".".join(f"{{{d.id}}}" for d in dimensions) if dimensions else None

    names: dict[str, str] = {}
    if stub.names:
        names.update(stub.names)
    if detail.names:
        for k, v in detail.names.items():
            names.setdefault(k, v)
    for lang, name in multilang_names.items():
        names[lang] = name

    queries = SPECIFIC_QUERIES.get((stub.agency, stub.id), GENERIC_QUERIES)

    # Note: NBB marks every published dataflow as ``isFinal: false`` (via an
    # ``NonProductionDataflow`` annotation). This is a platform convention, not a
    # real usability signal — all 221 flows are the canonical production data.
    # We therefore force ``is_final=True`` at build time. The field is still
    # preserved on the model in case a future agency sends real final/non-final
    # distinctions.
    return EnrichedDataflow(
        agency=stub.agency,
        id=stub.id,
        version=stub.version,
        category=category,
        is_final=True,
        names=names,
        frequencies_available=freq_codes,
        default_frequency=_detect_default_frequency(freq_codes),
        time_coverage=TimeCoverage(),
        dimensions=dimensions,
        primary_measure=primary_measure,
        key_template=key_template,
        common_queries=list(queries),
        etag=None,
        fetched_at=datetime.now(UTC).isoformat(timespec="seconds"),
        schema_version=SCHEMA_VERSION,
    )


def _fiche_path(catalog_dir: Path, agency: str, dataflow_id: str) -> Path:
    return catalog_dir / f"{agency}_{dataflow_id}.json"


def _write_atomic(path: Path, payload: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload)
    tmp.replace(path)


# Fields that change on every run but carry no real payload — ignored when
# deciding whether a fiche has actually drifted.
_VOLATILE_FIELDS = frozenset({"fetched_at"})


def _fiche_content_equal(existing_raw: str, new_fiche: EnrichedDataflow) -> bool:
    """Return True if ``new_fiche`` is semantically identical to an on-disk fiche.

    The check ignores volatile fields (currently ``fetched_at``) so that a
    weekly rebuild without upstream changes produces an empty diff.
    """
    try:
        existing = json.loads(existing_raw)
    except ValueError:
        return False
    new_data = new_fiche.model_dump(mode="json")
    for f in _VOLATILE_FIELDS:
        existing.pop(f, None)
        new_data.pop(f, None)
    return existing == new_data


# --------------------------------------------------------------------------- #
# Orchestration.
# --------------------------------------------------------------------------- #


async def _fetch_multilang_names(client: NBBClient) -> dict[tuple[str, str], dict[str, str]]:
    """Fetch the dataflow list in 4 languages and merge the localised names.

    Returns a dict keyed by ``(agency, id)`` → ``{lang: name}``.
    """
    merged: dict[tuple[str, str], dict[str, str]] = {}
    for lang in SUPPORTED_LANGUAGES:
        try:
            payload = await client.list_dataflows(language=lang)
        except NBBError as exc:
            log.warning("catalog.multilang.failed", language=lang, error=str(exc))
            continue
        stubs = parse_dataflow_list(payload)
        for s in stubs:
            key = (s.agency, s.id)
            entry = merged.setdefault(key, {})
            if s.name:
                entry[lang] = s.name
            for k, v in s.names.items():
                entry[k] = v
    return merged


async def _build_one(
    client: NBBClient,
    stub: DataflowStub,
    catalog_dir: Path,
    multilang: dict[tuple[str, str], dict[str, str]],
    sem: asyncio.Semaphore,
    *,
    force: bool,
) -> tuple[str, str | None]:
    path = _fiche_path(catalog_dir, stub.agency, stub.id)
    if path.exists() and not force:
        return ("skipped", None)

    category = classify(stub)

    async with sem:
        try:
            payload = await client.get_dataflow(stub.agency, stub.id, stub.version)
        except NBBError as exc:
            log.error(
                "catalog.fetch.failed",
                agency=stub.agency,
                id=stub.id,
                code=exc.code,
                message=exc.message,
            )
            return ("error", f"{exc.code}: {exc.message}")

    try:
        detail = parse_dataflow_detail(payload)
    except NBBError as exc:
        log.error("catalog.parse.failed", agency=stub.agency, id=stub.id, error=str(exc))
        return ("error", f"parse: {exc.message}")

    fiche = build_fiche(
        stub,
        detail,
        category=category,
        multilang_names=multilang.get((stub.agency, stub.id), {}),
    )

    # Skip the write if the only thing that would change is the fetched_at
    # timestamp — otherwise every weekly rebuild would produce a noisy 200+
    # file PR even when NBB hasn't published anything new.
    if path.exists():
        existing_raw = path.read_text()
        if _fiche_content_equal(existing_raw, fiche):
            return ("unchanged", None)

    _write_atomic(path, fiche.model_dump_json(indent=2))
    return ("built", None)


def _write_index(
    catalog_dir: Path,
    stubs: list[DataflowStub],
    started_at: str,
    *,
    any_fiche_written: bool,
) -> None:
    """Write ``_index.json`` — but only bump ``built_at`` if at least one fiche drifted.

    This keeps the diff empty on a no-op run (nothing in git status), so the
    weekly rebuild workflow never opens a spurious PR.
    """
    categories: Counter[str] = Counter(classify(s) for s in stubs)
    flows = [
        {
            "agency": s.agency,
            "id": s.id,
            "version": s.version,
            "category": classify(s),
            "is_final": True,
        }
        for s in sorted(stubs, key=lambda x: (x.agency, x.id))
    ]

    index_path = catalog_dir / "_index.json"
    built_at = started_at
    if index_path.exists() and not any_fiche_written:
        try:
            existing = json.loads(index_path.read_text())
            if existing.get("built_at"):
                built_at = existing["built_at"]
        except ValueError:
            pass

    index = {
        "built_at": built_at,
        "dataflow_count": len(stubs),
        "schema_version": SCHEMA_VERSION,
        "categories": dict(sorted(categories.items())),
        "flows": flows,
    }
    new_payload = json.dumps(index, indent=2, ensure_ascii=False)
    if index_path.exists():
        try:
            if index_path.read_text() == new_payload:
                return
        except OSError:
            pass
    _write_atomic(index_path, new_payload)


def _write_errors_report(catalog_dir: Path, errors: list[dict[str, Any]]) -> None:
    if not errors:
        path = catalog_dir / "_build_errors.json"
        if path.exists():
            path.unlink()
        return
    path = catalog_dir / "_build_errors.json"
    _write_atomic(path, json.dumps(errors, indent=2, ensure_ascii=False))


def _default_catalog_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "catalog"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the nbb-mcp enriched catalogue")
    parser.add_argument(
        "--force", action="store_true", help="Rebuild all fiches, ignoring existing ones"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Only process the first N dataflows (debug)"
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Comma-separated AGENCY/ID pairs to rebuild (e.g. BE2/DF_EXR,BE2/DF_HICP_2025)",
    )
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Target directory (defaults to <package>/data/catalog)",
    )
    return parser.parse_args(argv)


async def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    settings: Settings = get_settings()
    configure_logging(level=settings.log_level, fmt="console")

    catalog_dir = args.output_dir or _default_catalog_dir()
    catalog_dir.mkdir(parents=True, exist_ok=True)

    log.info("catalog.build.start", output_dir=str(catalog_dir), concurrency=args.concurrency)
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    t0 = time.monotonic()

    async with nbb_client(settings) as client:
        list_payload = await client.list_dataflows()
        stubs = parse_dataflow_list(list_payload)
        log.info("catalog.list.fetched", count=len(stubs))

        if args.only:
            wanted = {tuple(pair.split("/", 1)) for pair in args.only.split(",") if pair.strip()}
            stubs = [s for s in stubs if (s.agency, s.id) in wanted]
            log.info("catalog.filter.only", remaining=len(stubs))
        if args.limit:
            stubs = stubs[: args.limit]
            log.info("catalog.filter.limit", remaining=len(stubs))

        if not stubs:
            log.warning("catalog.empty_selection")
            return 0

        multilang = await _fetch_multilang_names(client)
        log.info("catalog.multilang.merged", flows_with_names=len(multilang))

        sem = asyncio.Semaphore(args.concurrency)
        tasks = [
            _build_one(client, stub, catalog_dir, multilang, sem, force=args.force)
            for stub in stubs
        ]

        results: list[tuple[str, str | None]] = []
        built = skipped = unchanged = errored = 0
        for i, coro in enumerate(asyncio.as_completed(tasks), start=1):
            status, err = await coro
            results.append((status, err))
            if status == "built":
                built += 1
            elif status == "unchanged":
                unchanged += 1
            elif status == "skipped":
                skipped += 1
            else:
                errored += 1
            if i % 20 == 0 or i == len(tasks):
                log.info(
                    "catalog.progress",
                    done=i,
                    total=len(tasks),
                    built=built,
                    unchanged=unchanged,
                    skipped=skipped,
                    errored=errored,
                )

    errors: list[dict[str, Any]] = []
    for stub in stubs:
        path = _fiche_path(catalog_dir, stub.agency, stub.id)
        if not path.exists():
            errors.append({"agency": stub.agency, "id": stub.id, "error": "fiche not written"})

    _write_index(catalog_dir, stubs, started_at, any_fiche_written=(built > 0))
    _write_errors_report(catalog_dir, errors)

    elapsed = time.monotonic() - t0
    log.info(
        "catalog.build.done",
        built=built,
        skipped=skipped,
        errored=errored,
        elapsed_s=round(elapsed, 2),
    )
    if errored:
        log.warning("catalog.build.errors_report", path=str(catalog_dir / "_build_errors.json"))
        return 1
    return 0


def main() -> None:
    try:
        rc = asyncio.run(run())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
