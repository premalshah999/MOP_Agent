from __future__ import annotations

import re

from app.schemas.query_plan import Intent


def classify_intent(question: str) -> dict:
    q = question.strip().lower()
    if not q:
        return {"intent": "AMBIGUOUS", "requires_sql": False, "requires_multiple_queries": False, "reason": "Empty question."}

    if q in {"hi", "hello", "hey"} or any(phrase in q for phrase in ("who are you", "what are you", "what can you do", "help")):
        return {"intent": "DEFINITION", "requires_sql": False, "requires_multiple_queries": False, "reason": "Assistant identity/help question."}
    if any(phrase in q for phrase in ("what years", "which years", "available years", "what metrics", "available metrics", "what datasets")):
        return {"intent": "DEFINITION", "requires_sql": False, "requires_multiple_queries": False, "reason": "Metadata availability request."}
    if any(phrase in q for phrase in ("what does", "define", "definition of", "what is the meaning")):
        return {"intent": "DEFINITION", "requires_sql": False, "requires_multiple_queries": False, "reason": "Definition request."}
    if any(phrase in q for phrase in ("why did", "why has", "root cause", "what drove", "driver")):
        return {"intent": "ROOT_CAUSE", "requires_sql": True, "requires_multiple_queries": True, "reason": "Diagnostic/root-cause wording."}
    if any(phrase in q for phrase in ("trend", "over time", "changed over", "last 12 months", "year over year")):
        return {"intent": "TREND", "requires_sql": True, "requires_multiple_queries": False, "reason": "Trend wording."}
    if any(phrase in q for phrase in ("compare", " versus ", " vs ", "against")):
        return {"intent": "COMPARISON", "requires_sql": True, "requires_multiple_queries": False, "reason": "Comparison wording."}
    if any(phrase in q for phrase in ("top", "bottom", "highest", "lowest", "maximum", "minimum", "max", "min", "most", "least", "rank", "largest", "biggest", "smallest", "best", "worst")):
        return {"intent": "AGGREGATION", "requires_sql": True, "requires_multiple_queries": False, "reason": "Ranking/aggregation wording."}
    if re.search(r"\bby\b", q) or any(phrase in q for phrase in ("breakdown", "broken down")):
        return {"intent": "BREAKDOWN", "requires_sql": True, "requires_multiple_queries": False, "reason": "Breakdown/grouping wording."}
    if any(phrase in q for phrase in ("how much", "how many", "what was", "what is", "where does", "show me", "give me")):
        return {"intent": "DIRECT_LOOKUP", "requires_sql": True, "requires_multiple_queries": False, "reason": "Lookup wording."}
    return {"intent": "AMBIGUOUS", "requires_sql": False, "requires_multiple_queries": False, "reason": "No supported analytical intent detected."}
