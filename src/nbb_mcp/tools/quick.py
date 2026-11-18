"""``nbb_quick`` — topic-based shortcuts onto the headline NBB series.

Each topic pins a dataflow and the dimension codes of its headline series
(e.g. GDP → quarterly, chain-linked volume, seasonally adjusted, total
economy), so a call returns one meaningful series instead of every series in
the flow. ``frequency`` overrides the ``FREQ`` code on any topic; ``currency``
only applies to ``exchange_rate``. All codes are validated against the
bundled catalogue, which lists only codes that actually hold data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..formatter import DEFAULT_MAX_SERIES, format_data_message, validate_format
from ..models.errors import NBBValidationError
from ..parsers.sdmx_json_v2 import parse_data_message
from ..query_builder import build_sdmx_key
from ..services import get_services
from .query import validate_obs_counts

# Without a period window, a quick call returns this many latest observations.
DEFAULT_LAST_N = 24


@dataclass(frozen=True)
class Topic:
    dataflow_id: str
    description: str
    defaults: dict[str, str] = field(default_factory=dict)
    frequency: str | None = None
    agency: str = "BE2"
    accepts_currency: bool = False


TOPICS: dict[str, Topic] = {
    # Exchange rates --------------------------------------------------------
    "exchange_rate": Topic(
        "DF_EXR",
        "EUR reference exchange rate against `currency` (default USD), daily",
        defaults={"EXR_CURRENCY": "USD"},
        frequency="D",
        accepts_currency=True,
    ),
    # Interest rates --------------------------------------------------------
    "policy_rate": Topic(
        "DF_IRESCB",
        "ECB deposit facility rate (the operative policy rate), daily",
        defaults={"IRESCB_RATE": "DPF"},
        frequency="D",
    ),
    "mortgage_rate": Topic(
        "DF_MIR",
        "Cost-of-borrowing indicator for households for house purchase (ECB composite), monthly",
        defaults={
            "MIR_ITEM": "_Z",
            "MIR_SECTOR": "_Z",
            "MIR_INSTRUMENT": "CB_1",
            "MIR_MATURITY": "Z",
            "MIR_QUARTILE": "Z",
            "MIR_FACTOR": "Z",
        },
        frequency="M",
    ),
    "long_term_yield": Topic(
        "DF_IROLOYLD",
        "Belgian 10-year reference loan (OLO) yield, daily",
        defaults={"IROLOYLD_AREA": "BE"},
        frequency="D",
    ),
    # Prices ----------------------------------------------------------------
    "inflation_hicp": Topic(
        "DF_HICP_2025",
        "Belgian HICP inflation, all items, % change, monthly",
        defaults={
            "REF_AREA": "BE",
            "NBB_COICOP": "000000",
            "INDEX_TYPE": "HCP",
            "DERIVATION": "GROWTH_RATE",
        },
        frequency="M",
    ),
    "inflation_national": Topic(
        "DF_NICP_2025",
        "Belgian national CPI inflation, general index, % change, monthly",
        defaults={"NBB_COICOP": "99_9_9_9_99", "DERIVATION": "GROWTH_RATE"},
        frequency="M",
    ),
    "ppi": Topic(
        "DF_PPI",
        "Producer price index, general index, total market, monthly",
        defaults={"MARKET_GEO": "OPI", "NBB_ISTI": "20"},
        frequency="M",
    ),
    "industrial_production": Topic(
        "DF_INDPROD",
        "Industrial production index, total industry (B-D), Belgium, calendar adjusted",
        defaults={
            "INDEX_TYPE": "INDPROD",
            "ADJUSTMENT": "W",
            "NBB_PROD_SECTOR": "B_C_D",
            "BE_AREA": "BE",
        },
        frequency="M",
    ),
    # Real economy ----------------------------------------------------------
    "gdp": Topic(
        "DF_QNA_DISS",
        "Belgian GDP, chain-linked volume (EUR million), seasonally adjusted, quarterly",
        defaults={
            "ACCOUNT": "2",
            "INDICATOR": "B1GM",
            "PRICES": "L",
            "ADJUSTMENT": "Y",
        },
        frequency="Q",
    ),
    "gdp_growth": Topic(
        "DF_QNA_DISS",
        "Belgian real GDP growth, quarter-on-quarter %, seasonally adjusted",
        defaults={
            "ACCOUNT": "2",
            "INDICATOR": "B1GM",
            "PRICES": "LQ",
            "ADJUSTMENT": "Y",
        },
        frequency="Q",
    ),
    "unemployment_rate": Topic(
        "DF_UNEMPLOY_RATE",
        "Belgian harmonised unemployment rate, total, seasonally adjusted, monthly",
        defaults={
            "GENDER": "AA",
            "AGE": "Z0000",
            "ADJUSTMENT": "Y",
            "REGION": "BE",
        },
        frequency="M",
    ),
    "employment": Topic(
        "DF_EMPLOY_DISS",
        "Total domestic employment (thousands of persons), seasonally adjusted, quarterly",
        defaults={
            "INDICATOR": "TOTEMP",
            "BRANCH": "VZ",
            "CONCEPT": "D",
            "ADJUSTMENT": "Y",
        },
        frequency="Q",
    ),
    # Public finances -------------------------------------------------------
    "government_debt": Topic(
        "DF_CGD",
        "Consolidated gross debt of general government, total instruments, quarterly",
        defaults={"ITEM": "CGD", "GSECTOR": "S1300", "INSTRUMENT": "F"},
        frequency="Q",
    ),
    "government_deficit": Topic(
        "DF_NFGOV_NET_DISS",
        "General government net lending (+) / borrowing (-), % of GDP, annual",
        defaults={"GOVTYPE": "S1300", "ITEM": "DEF", "UNIT": "P"},
        frequency="A",
    ),
    "current_account": Topic(
        "DF_BOPBPM6",
        "Balance of payments — current account, net, quarterly",
        defaults={"BOPRESERV_ITEM": "993", "BOPBPM6_FLOW": "04"},
        frequency="Q",
    ),
    # Surveys ---------------------------------------------------------------
    "consumer_confidence": Topic(
        "DF_CONSN",
        "Consumer confidence indicator, Belgium, monthly",
        defaults={"CONSN_INDIC": "CCI", "BE_AREA": "BE"},
        frequency="M",
    ),
    "business_confidence": Topic(
        "DF_BUSSURVM",
        "Business barometer (synthetic curve, smoothed), all sectors, Belgium, monthly",
        defaults={
            "BUSSURVM_INDICATOR": "SYNC",
            "BE_AREA": "BE",
            "BUSSURV_SECTOR": "A999",
            "BUSSURVM_ADJ": "X",
        },
        frequency="M",
    ),
    # Trade -----------------------------------------------------------------
    "trade_balance": Topic(
        "DF_EXTERNAL_TRADE_OVERVIEW",
        "Belgian goods trade balance with the world, national concept, monthly value",
        defaults={
            "AREA": "NBB_A1",
            "FLOW": "B",
            "CONCEPT": "NAT",
            "DATATYPE": "VAL",
            "UNIT": "M",
        },
        frequency="M",
    ),
}


def list_topics() -> dict[str, dict[str, str]]:
    return {
        name: {
            "agency": t.agency,
            "dataflow_id": t.dataflow_id,
            "description": t.description,
        }
        for name, t in TOPICS.items()
    }


def topic_filters(
    topic: Topic, *, currency: str | None = None, frequency: str | None = None
) -> dict[str, str]:
    """Resolve the SDMX filters of a topic call (pure — no catalogue access)."""
    if currency is not None and not topic.accepts_currency:
        raise NBBValidationError(
            f"'currency' does not apply to {topic.dataflow_id}",
            code="UNSUPPORTED_ARG",
            details={"topics_accepting_currency": ["exchange_rate"]},
        )
    filters = dict(topic.defaults)
    if currency:
        filters["EXR_CURRENCY"] = currency.strip().upper()
    freq = (frequency or topic.frequency or "").strip().upper()
    if freq:
        filters["FREQ"] = freq
    return filters


async def run(
    topic: str,
    *,
    currency: str | None = None,
    frequency: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    last_n_observations: int | None = None,
    max_observations: int = 200,
    max_series: int = DEFAULT_MAX_SERIES,
    format: str = "summary",
) -> dict[str, Any]:
    if topic not in TOPICS:
        raise NBBValidationError(
            f"Unknown topic '{topic}'",
            code="UNKNOWN_TOPIC",
            details={"valid_topics": sorted(TOPICS)},
        )
    validate_format(format)
    validate_obs_counts(last_n_observations)
    cfg = TOPICS[topic]

    svc = get_services()
    entry = svc.catalog.get(cfg.agency, cfg.dataflow_id)
    filters = topic_filters(cfg, currency=currency, frequency=frequency)
    key = build_sdmx_key(entry, filters)

    if last_n_observations is None and start_period is None and end_period is None:
        last_n_observations = DEFAULT_LAST_N

    payload = await svc.client.fetch_data(
        cfg.agency,
        cfg.dataflow_id,
        entry.version,
        key,
        start_period=start_period,
        end_period=end_period,
        last_n_observations=last_n_observations,
    )
    dataflow_ref = f"{cfg.agency}/{cfg.dataflow_id}/{entry.version}"
    msg = parse_data_message(payload, dataflow=dataflow_ref)
    response = format_data_message(
        msg,
        fmt=format,
        max_observations=max_observations,
        max_series=max_series,
        dataflow_ref=dataflow_ref,
        key=key,
        entry=entry,
    )
    payload_out = response.model_dump(exclude_none=True)
    payload_out["topic"] = topic
    payload_out["topic_description"] = cfg.description
    payload_out["filters"] = filters
    return payload_out
