from __future__ import annotations

from difflib import get_close_matches
import re
from dataclasses import dataclass


US_STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district of columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
]

STATE_TO_POSTAL = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}

_NORMALIZER = re.compile(r"[^a-z0-9]+")
_EXPLICIT_FLOW_PATTERNS = (
    "fund flow",
    "subaward",
    "subcontract",
    "inflow",
    "outflow",
    "origin",
    "destination",
    "awardee",
    "naics",
)
_FINRA_PATTERNS = (
    "finra",
    "financial literacy",
    "financial constraint",
    "alternative financing",
    "risk averse",
    "risk aversion",
    "satisfied",
)
_GOV_PATTERNS = (
    "government finance",
    "liabilities",
    "liability",
    "assets",
    "revenue",
    "expenses",
    "debt ratio",
    "current ratio",
    "net position",
    "pension",
    "opeb",
    "free cash flow",
)
_ACS_PATTERNS = (
    "census",
    "acs",
    "population",
    "poverty",
    "median household income",
    "education",
    "hispanic",
    "black",
    "asian",
    "white",
    "owner occupied",
    "renter occupied",
    "household",
)
_BREAKDOWN_PATTERNS = (
    "breakdown",
    "spending bars",
    "jobs bars",
    "funding category",
    "composite spending",
)
_FEDERAL_PATTERNS = (
    "federal spending",
    "federal money",
    "federal funding",
    "contracts",
    "contract",
    "grants",
    "grant",
    "direct payments",
    "resident wage",
    "employees wage",
    "federal residents",
    "agency",
    "agencies",
    "department",
    "spending",
)
_RANKING_PATTERNS = ("top", "bottom", "highest", "lowest", "most", "least", "largest", "smallest", "rank", "leading")
_TREND_PATTERNS = ("trend", "over time", "changed", "change", "by year", "time series")
_COMPARE_PATTERNS = ("compare", "versus", " vs ", "against")
_SHARE_PATTERNS = ("share", "percent", "percentage")

_METRIC_PATTERNS: tuple[tuple[str, str], ...] = (
    ("free cash flow", "Free_Cash_Flow"),
    ("debt ratio", "Debt_Ratio"),
    ("current ratio", "Current_Ratio"),
    ("net position", "Net_Position"),
    ("total liabilities", "Total_Liabilities"),
    ("liabilities", "Total_Liabilities"),
    ("total assets", "Total_Assets"),
    ("assets", "Total_Assets"),
    ("revenue per capita", "Revenue_per_capita"),
    ("revenue", "Revenue"),
    ("expenses", "Expenses"),
    ("net pension liability", "Net_Pension_Liability"),
    ("financial literacy", "financial_literacy"),
    ("financial constraint", "financial_constraint"),
    ("alternative financing", "alternative_financing"),
    ("risk averse", "risk_averse"),
    ("risk aversion", "risk_averse"),
    ("satisfied", "satisfied"),
    ("median household income", "Median household income"),
    ("below poverty", "Below poverty"),
    ("poverty", "Below poverty"),
    ("education >= bachelor", "Education >= Bachelor's"),
    ("bachelor", "Education >= Bachelor's"),
    ("owner occupied", "Owner occupied"),
    ("age 18 65", "Age 18-65"),
    ("total population", "Total population"),
    ("population", "Total population"),
    ("household", "# of household"),
    ("income > 100k", "Income >$100K"),
    ("income >$100k", "Income >$100K"),
    ("income above 100k", "Income >$100K"),
    ("income above $100k", "Income >$100K"),
    ("income > 200k", "Income >$200K"),
    ("income >$200k", "Income >$200K"),
    ("income above 200k", "Income >$200K"),
    ("income above $200k", "Income >$200K"),
    ("income > 50k", "Income >$50K"),
    ("income >$50k", "Income >$50K"),
    ("income above 50k", "Income >$50K"),
    ("income above $50k", "Income >$50K"),
    ("contracts", "Contracts"),
    ("grants", "Grants"),
    ("resident wage", "Resident Wage"),
    ("direct payments", "Direct Payments"),
    ("employees wage", "Employees Wage"),
    ("jobs", "Employees"),
    ("employment", "Employees"),
    ("employees", "Employees"),
    ("federal residents", "Federal Residents"),
)


@dataclass(frozen=True)
class QueryFrame:
    question: str
    normalized_question: str
    family: str | None
    geo_level: str | None
    state_names: tuple[str, ...]
    primary_state: str | None
    state_postal: str | None
    period_label: str | None
    intent: str
    wants_relative: bool
    wants_agency_dimension: bool
    wants_industry_dimension: bool
    wants_jobs_metric: bool
    wants_pair_ranking: bool
    wants_internal_flow: bool
    wants_displayed_flow: bool
    flow_direction: str | None
    metric_hint: str | None


def normalize_question(text: str) -> str:
    return _NORMALIZER.sub(" ", text.lower()).strip()


def detect_geo_level(question: str) -> str | None:
    q = question.lower()
    if "county" in q or "counties" in q:
        return "county"
    if "district" in q or "congress" in q:
        return "congress"
    if "state" in q or "states" in q:
        return "state"
    return None


def extract_state_names(question: str) -> tuple[str, ...]:
    q = question.lower()
    found: list[str] = []
    for state in sorted(US_STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(state)}\b", q):
            found.append(state)
    if found:
        return tuple(found)

    normalized = re.sub(r"[^a-z0-9\s]+", " ", q)
    tokens = [token for token in normalized.split() if token]
    fuzzy_matches: list[str] = []
    for size in (3, 2, 1):
        for start in range(0, len(tokens) - size + 1):
            candidate = " ".join(tokens[start : start + size]).strip()
            if len(candidate) < 5:
                continue
            match = get_close_matches(candidate, US_STATE_NAMES, n=1, cutoff=0.88)
            if match and match[0] not in fuzzy_matches:
                fuzzy_matches.append(match[0])
    if fuzzy_matches:
        return tuple(fuzzy_matches)
    return tuple(found)


def extract_state_name(question: str) -> str | None:
    names = extract_state_names(question)
    return names[0] if names else None


def state_postal_code(question: str) -> str | None:
    state = extract_state_name(question)
    return STATE_TO_POSTAL.get(state) if state else None


def extract_period_label(question: str) -> str | None:
    period_match = re.search(r"\b(2020-2024|Fiscal Year 20\d{2}|19\d{2}|20\d{2})\b", question, re.IGNORECASE)
    if not period_match:
        return None
    label = period_match.group(1)
    if label.lower().startswith("fiscal year"):
        return f"Fiscal Year {label.split()[-1]}"
    return label


def extract_year(question: str) -> str | None:
    period = extract_period_label(question)
    if period and period.isdigit():
        return period
    return None


def _family_scores(q: str) -> dict[str, int]:
    scores = {family: 0 for family in ("acs", "gov", "finra", "contract", "agency", "breakdown", "flow")}

    if any(pattern in q for pattern in _EXPLICIT_FLOW_PATTERNS):
        scores["flow"] += 5
    if any(pattern in q for pattern in _FINRA_PATTERNS):
        scores["finra"] += 5
    if any(pattern in q for pattern in _GOV_PATTERNS):
        scores["gov"] += 5
    if any(pattern in q for pattern in _ACS_PATTERNS):
        scores["acs"] += 4
    if any(pattern in q for pattern in _BREAKDOWN_PATTERNS):
        scores["breakdown"] += 4
    if any(pattern in q for pattern in _FEDERAL_PATTERNS):
        scores["contract"] += 3
        if any(token in q for token in ("agency", "agencies", "department")):
            scores["agency"] += 5

    if "jobs" in q or "employees" in q:
        scores["agency"] += 1
        scores["breakdown"] += 1

    # Guardrail: "free cash flow" belongs to government finance, not fund flow.
    if "free cash flow" in q:
        scores["gov"] += 5
        scores["flow"] = max(scores["flow"] - 3, 0)

    top_family = max(scores, key=scores.get)
    return scores if scores[top_family] > 0 else {}


def _infer_family(q: str) -> str | None:
    scores = _family_scores(q)
    if not scores:
        return None
    return max(scores, key=scores.get)


def _infer_intent(q: str, state_names: tuple[str, ...]) -> str:
    if any(token in q for token in _TREND_PATTERNS):
        return "trend"
    if any(token in q for token in _SHARE_PATTERNS):
        return "share"
    if len(state_names) >= 2 or any(token in q for token in _COMPARE_PATTERNS):
        return "compare"
    if any(token in q for token in _RANKING_PATTERNS):
        return "ranking"
    return "lookup"


def _infer_flow_direction(q: str) -> str | None:
    if any(token in q for token in (" inflow", " inflows", "into ", "incoming", "receive", "received")):
        return "inflow"
    if any(token in q for token in (" outflow", " outflows", "from ", "outbound", "leaving", "send ", "sends ", "sent ")):
        return "outflow"
    return None


def _infer_metric_hint(q: str) -> str | None:
    best_match: tuple[int, str] | None = None
    for pattern, metric in _METRIC_PATTERNS:
        if pattern in q:
            score = len(pattern)
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    if best_match is not None:
        return best_match[1]
    if "federal money" in q or "federal funding" in q or "default spending" in q:
        return "spending_total"
    if "spending" in q and not any(token in q for token in ("direct payments", "resident wage", "employees wage")):
        return "spending_total"
    return None


def infer_query_frame(question: str) -> QueryFrame:
    q = normalize_question(question)
    states = extract_state_names(question)
    primary_state = states[0] if states else None
    postal = STATE_TO_POSTAL.get(primary_state) if primary_state else None

    return QueryFrame(
        question=question,
        normalized_question=q,
        family=_infer_family(q),
        geo_level=detect_geo_level(question),
        state_names=states,
        primary_state=primary_state,
        state_postal=postal,
        period_label=extract_period_label(question),
        intent=_infer_intent(q, states),
        wants_relative=("per capita" in q or "per 1000" in q or "relative exposure" in q),
        wants_agency_dimension=any(token in q for token in ("agency", "agencies", "department")),
        wants_industry_dimension=any(token in q for token in ("industry", "industries", "sector", "sectors", "naics")),
        wants_jobs_metric=any(token in q for token in ("jobs", "employees", "employment", "workforce")),
        wants_pair_ranking=any(token in q for token in ("pair", "pairs", "between")) or ("largest flow" in q or "biggest flow" in q),
        wants_internal_flow=("internal flow" in q or "stays within" in q or "within maryland" in q and "flow" in q),
        wants_displayed_flow=("displayed flow" in q or "shown on the map" in q or "shown on map" in q),
        flow_direction=_infer_flow_direction(q),
        metric_hint=_infer_metric_hint(q),
    )
