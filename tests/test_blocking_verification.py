from __future__ import annotations

from app.core import orchestrator


def _route():
    return {
        "intent": "ANALYTICAL",
        "requires_sql": True,
        "needs_clarification": False,
        "clarification_question": "",
        "clarification": "",
        "reason": "clear",
        "tables": ["contract_state"],
        "columns": ["Grants"],
        "geography_level": "state",
        "year_strategy": "2024",
        "join_plan": "",
        "confidence": "high",
        "operation": "lookup",
        "flow_direction": "none",
        "sort_direction": "none",
        "top_k": None,
        "assumptions": [],
    }


def _wire_pipeline(monkeypatch, verdicts):
    monkeypatch.setattr(orchestrator, "contextualize", lambda question, history: question)
    monkeypatch.setattr(orchestrator, "classify_and_route", lambda question, history: _route())
    monkeypatch.setattr(
        orchestrator,
        "build_grounding",
        lambda *args, **kwargs: {
            "text": "grounding",
            "resolved": {"contract_state": {"state": {"value": "MARYLAND", "values": ["MARYLAND"]}}},
        },
    )
    monkeypatch.setattr(orchestrator, "match_verified_query", lambda question: None)
    monkeypatch.setattr(
        orchestrator,
        "generate_and_execute",
        lambda *args, **kwargs: {
            "sql": 'SELECT state, "Grants" FROM mart_contract_state WHERE state=\'MARYLAND\' AND year=\'2024\'',
            "rows": [{"state": "MARYLAND", "Grants": 100.0}],
            "error": None,
            "attempts": [],
        },
    )
    answers = iter(
        [
            {"answer": "Wrong draft: **$999**", "key_numbers": [], "caveats": [], "confidence": "high"},
            {"answer": "Corrected: **$100**", "key_numbers": [], "caveats": [], "confidence": "high"},
        ]
    )
    monkeypatch.setattr(orchestrator, "write_answer", lambda *args, **kwargs: next(answers))
    monkeypatch.setattr(orchestrator, "judge_faithfulness", lambda *args, **kwargs: next(verdicts))
    monkeypatch.setattr(orchestrator, "compute_peer_context", lambda **kwargs: None)
    monkeypatch.setattr(orchestrator, "enrich_rows_for_map", lambda q, r, g, rows: rows)
    monkeypatch.setattr(
        orchestrator,
        "build_visuals",
        lambda *args, **kwargs: {
            "chart": None,
            "charts": [],
            "map_intent": {"enabled": False, "mapType": "none"},
        },
    )
    monkeypatch.setattr(orchestrator, "suggest_followups", lambda *args, **kwargs: [])


def test_failed_draft_is_repaired_before_any_preview(monkeypatch):
    verdicts = iter(
        [
            {"faithful": False, "available": True, "reason": "999 is unsupported"},
            {"faithful": True, "available": True, "reason": "supported"},
        ]
    )
    _wire_pipeline(monkeypatch, verdicts)
    events = []
    result = orchestrator.answer_question(
        "How many grant dollars did Maryland receive?",
        on_event=lambda name, data: events.append((name, data)),
    )
    previews = [payload for name, payload in events if name == "answer_preview"]
    assert result["resolution"] == "answered"
    assert result["answer"] == "Corrected: **$100**"
    assert [preview["answer"] for preview in previews] == ["Corrected: **$100**"]


def test_twice_failed_answer_is_withheld(monkeypatch):
    verdicts = iter(
        [
            {"faithful": False, "available": True, "reason": "unsupported draft"},
            {"faithful": False, "available": True, "reason": "still unsupported"},
        ]
    )
    _wire_pipeline(monkeypatch, verdicts)
    events = []
    result = orchestrator.answer_question(
        "How many grant dollars did Maryland receive?",
        on_event=lambda name, data: events.append((name, data)),
    )
    previews = [payload for name, payload in events if name == "answer_preview"]
    assert result["resolution"] == "verification_failed"
    assert "could not safely verify" in result["answer"]
    assert all("Wrong draft" not in preview["answer"] for preview in previews)
    assert result["contract"]["supported"] is False
