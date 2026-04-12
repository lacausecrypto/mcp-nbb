"""``nbb_quick`` — topic-based shortcuts onto well-known NBB dataflows.

Each topic resolves at runtime to ``(agency, dataflow_id, filter_builder)``.
The ``filter_builder`` receives the tool kwargs and returns a dict of
``{dimension_id: code}`` pairs which are then validated against the local
catalogue. If a topic maps to a flow whose dimension ids are unknown at static
time, the builder returns an empty dict and the fetch falls back to a
wildcarded key, usually narrowed by ``start_period``/``end_period`` or
``last_n_observations``.
"""

from __future__ import annotations

from typing import Any

from ..formatter import format_data_message
from ..models.errors import NBBValidationError
from ..parsers.sdmx_json_v2 import parse_data_message
from ..query_builder import build_sdmx_key
from ..services import get_services

# ----------------------------------------------------------------------------
# Topic registry.
# ----------------------------------------------------------------------------


def _exr_filters(kw: dict[str, Any]) -> dict[str, str]:
    return {
        "FREQ": kw.get("frequency") or "D",
        "EXR_CURRENCY": kw.get("currency") or "USD",
    }


def _no_filters(_kw: dict[str, Any]) -> dict[str, str]:
    return {}


TopicEntry = dict[str, Any]


TOPICS: dict[str, TopicEntry] = {
    # Exchange rates --------------------------------------------------------
    "exchange_rate": {
        "agency": "BE2",
        "id": "DF_EXR",
        "filter_builder": _exr_filters,
        "description": "EUR exchange rates against another currency (DF_EXR)",
    },
    # Interest rates --------------------------------------------------------
    "policy_rate": {
        "agency": "BE2",
        "id": "DF_IRESCB",
        "filter_builder": _no_filters,
        "description": "ECB main policy interest rates (DF_IRESCB)",
    },
    "mortgage_rate": {
        "agency": "BE2",
        "id": "DF_MIR",
        "filter_builder": _no_filters,
        "description": "MFI interest rates on new business (DF_MIR)",
    },
    "long_term_yield": {
        "agency": "BE2",
        "id": "DF_IROLOYLD",
        "filter_builder": _no_filters,
        "description": "Belgian long-term reference loan yield (DF_IROLOYLD)",
    },
    # Prices ----------------------------------------------------------------
    "inflation_hicp": {
        "agency": "BE2",
        "id": "DF_HICP_2025",
        "filter_builder": _no_filters,
        "description": "Harmonised consumer price index, base 2025 (DF_HICP_2025)",
    },
    "inflation_national": {
        "agency": "BE2",
        "id": "DF_NICP_2025",
        "filter_builder": _no_filters,
        "description": "National consumer price index, base 2025 (DF_NICP_2025)",
    },
    "ppi": {
        "agency": "BE2",
        "id": "DF_PPI",
        "filter_builder": _no_filters,
        "description": "Producer price indices (DF_PPI)",
    },
    "industrial_production": {
        "agency": "BE2",
        "id": "DF_INDPROD",
        "filter_builder": _no_filters,
        "description": "Industrial production indices (DF_INDPROD)",
    },
    # Real economy ----------------------------------------------------------
    "gdp": {
        "agency": "BE2",
        "id": "DF_QNA_DISS",
        "filter_builder": _no_filters,
        "description": "GDP — 3 approaches, quarterly/annual (DF_QNA_DISS)",
    },
    "gdp_growth": {
        "agency": "BE2",
        "id": "DF_QNA_DISS",
        "filter_builder": _no_filters,
        "description": "GDP growth (DF_QNA_DISS)",
    },
    "unemployment_rate": {
        "agency": "BE2",
        "id": "DF_UNEMPLOY_RATE",
        "filter_builder": _no_filters,
        "description": "Harmonised unemployment rate (DF_UNEMPLOY_RATE)",
    },
    "employment": {
        "agency": "BE2",
        "id": "DF_EMPLOY_DISS",
        "filter_builder": _no_filters,
        "description": "Employment — quarterly data (DF_EMPLOY_DISS)",
    },
    # Public finances -------------------------------------------------------
    "government_debt": {
        "agency": "BE2",
        "id": "DF_CGD",
        "filter_builder": _no_filters,
        "description": "Consolidated gross debt of general government (DF_CGD)",
    },
    "government_deficit": {
        "agency": "BE2",
        "id": "DF_NFGOV_NET_DISS",
        "filter_builder": _no_filters,
        "description": "Government deficit / surplus (DF_NFGOV_NET_DISS)",
    },
    "current_account": {
        "agency": "BE2",
        "id": "DF_BOPBPM6",
        "filter_builder": _no_filters,
        "description": "Balance of payments — current account (DF_BOPBPM6)",
    },
    # Surveys ---------------------------------------------------------------
    "consumer_confidence": {
        "agency": "BE2",
        "id": "DF_CONSN",
        "filter_builder": _no_filters,
        "description": "Consumer survey / confidence (DF_CONSN)",
    },
    "business_confidence": {
        "agency": "BE2",
        "id": "DF_BUSSURVM",
        "filter_builder": _no_filters,
        "description": "Monthly business survey (DF_BUSSURVM)",
    },
    # Trade -----------------------------------------------------------------
    "trade_balance": {
        "agency": "BE2",
        "id": "DF_EXTERNAL_TRADE_OVERVIEW",
        "filter_builder": _no_filters,
        "description": "Foreign trade overview (DF_EXTERNAL_TRADE_OVERVIEW)",
    },
}


def list_topics() -> dict[str, dict[str, str]]:
    return {
        name: {
            "agency": cfg["agency"],
            "dataflow_id": cfg["id"],
            "description": cfg["description"],
        }
        for name, cfg in TOPICS.items()
    }


async def run(
    topic: str,
    *,
    currency: str | None = None,
    frequency: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    max_observations: int = 200,
    format: str = "summary",
) -> dict[str, Any]:
    if topic not in TOPICS:
        raise NBBValidationError(
            f"Unknown topic '{topic}'",
            code="UNKNOWN_TOPIC",
            details={"valid_topics": sorted(TOPICS.keys())},
        )
    cfg = TOPICS[topic]
    agency = cfg["agency"]
    flow_id = cfg["id"]
    kw = {"currency": currency, "frequency": frequency}

    svc = get_services()
    entry = svc.catalog.get(agency, flow_id)

    raw_filters = cfg["filter_builder"](kw)
    # Ignore filters whose dimensions don't actually exist on this flow —
    # defends against catalogue drift and typos in the topic registry.
    real_dims = {d.id for d in entry.dimensions}
    filters = {k: v for k, v in raw_filters.items() if k in real_dims and v}
    key = build_sdmx_key(entry, filters) if filters else "all"

    # For unfiltered topics with no period window, cap volume to last 12 obs.
    if (
        not filters
        and last_n_observations is None
        and start_period is None
        and end_period is None
    ):
        last_n_observations = 12

    payload = await svc.client.fetch_data(
        agency,
        flow_id,
        entry.version,
        key,
        start_period=start_period,
        end_period=end_period,
        last_n_observations=last_n_observations,
    )
    dataflow_ref = f"{agency}/{flow_id}/{entry.version}"
    msg = parse_data_message(payload, dataflow=dataflow_ref)
    response = format_data_message(
        msg,
        fmt=format,
        max_observations=max_observations,
        dataflow_ref=dataflow_ref,
        key=key,
        entry=entry,
    )
    payload_out = response.model_dump(exclude_none=True)
    payload_out["topic"] = topic
    return payload_out
