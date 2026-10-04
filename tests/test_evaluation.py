"""Offline evaluation contracts: no provider keys, network, or model downloads."""
import copy
import json

import httpx
import pytest

from scripts.evaluate import (
    load_fixture,
    normalize,
    phrase_present,
    resolve_scope,
    run_evaluation,
    score_answer,
    summarize,
    validate_fixture,
    write_report,
)


def test_all_synthetic_packs_have_supported_heldout_facts_and_disjoint_scopes():
    fixture = load_fixture()
    assert len(fixture["scenarios"]) == 6
    assert sum(len(s["questions"]) for s in fixture["scenarios"]) == 12
    assert validate_fixture(fixture) == []


def test_alias_scoring_counts_facts_once_and_matches_token_boundaries():
    question = {"expected_facts": [
        {"id": "bake", "any_of": ["8 minutes", "eight minutes"]},
        {"id": "command", "any_of": ["orbitctl revert"]},
    ], "forbidden_facts": [{"id": "contamination", "any_of": ["QUARTZ-CLOSE-9"]}]}
    score = score_answer("Bake for eight minutes (8 minutes). Run ORBITCTL REVERT.", question)
    assert score["expected_fact_recall"] == 1
    assert score["forbidden_fact_matches"] == []
    assert not phrase_present("Wait for 18 minutes", "8 minutes")
    assert not phrase_present("forecast", "cast")
    assert normalize("Dark-frame\nCALIBRATION") == "dark frame calibration"
    assert score_answer("QUARTZ-CLOSE-9", question)["forbidden_fact_matches"] == ["contamination"]


def test_forbidden_match_is_explicitly_lexical_not_a_safety_judgment():
    question = {"expected_facts": [{"id": "safe", "any_of": ["safe"]}],
                "forbidden_facts": [{"id": "guard", "any_of": ["bypass guards"]}]}
    assert score_answer("Never bypass guards; stay safe.", question)["forbidden_fact_matches"] == ["guard"]


def test_validator_rejects_cross_pack_scope_reuse_and_unsupported_facts():
    fixture = copy.deepcopy(load_fixture())
    fixture["scenarios"][1]["scope"] = fixture["scenarios"][0]["scope"]
    fixture["scenarios"][0]["questions"][0]["expected_facts"][0]["any_of"] = ["invented fact 271828"]
    errors = validate_fixture(fixture)
    assert any("disjoint" in e for e in errors)
    assert any("unsupported" in e for e in errors)


def test_scope_identifiers_are_bounded_and_independent_between_runs():
    scenario = load_fixture()["scenarios"][0]
    assert resolve_scope(scenario, "run1") != resolve_scope(scenario, "run2")
    with pytest.raises(ValueError):
        resolve_scope(scenario, "../../bad")
    assert summarize([])["recall_delta"] is None


def test_offline_end_to_end_report_uses_private_heldout_calls_and_isolation_controls(tmp_path):
    fixture = load_fixture()
    scenario = fixture["scenarios"][0]
    requests = []
    indexed = 0

    def respond(request):
        nonlocal indexed
        if request.url.path == "/v1/graph/stats":
            return httpx.Response(200, json={"calls": indexed})
        payload = json.loads(request.content)
        requests.append(payload)
        assert payload["use_cache"] is False
        if payload["store"]:
            indexed += 1
        control = any(payload[key].endswith("-unseeded") for key in ("user_id", "feature_tag"))
        context = ["memory-1"] if payload["use_graph"] and not control else []
        return httpx.Response(200, json={
            "response": "orbitctl revert; config revert; flight number; 8 minutes; 20 percent; p99",
            "meta": {"context_used": context, "latency_ms": 10, "cost": 0.001},
        })

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(respond)) as client:
        report = run_evaluation(client, fixture, scenarios=[scenario], run_id="test-run")
    assert not report["errors"]
    assert len(report["results"]) == 4
    assert len(report["isolation_controls"]) == 2
    assert all(c["passed"] for c in report["isolation_controls"])
    assert all(req["store"] and not req["use_graph"] for req in requests[:4])
    assert all(not req["store"] for req in requests[4:])
    assert [r["mode"] for r in report["results"]] == ["graph_off", "graph_on", "graph_on", "graph_off"]
    assert report["summary"]["graph_on"]["answers_with_context"] == 2
    assert report["summary"]["graph_off"]["expected_fact_recall"] == 1
    output = tmp_path / "nested" / "report.json"
    write_report(report, output)
    assert json.loads(output.read_text())["fixture_sha256"] == report["fixture_sha256"]
    assert "Bearer" not in output.read_text()


def test_transport_errors_are_reported_without_being_scored_as_model_failures():
    fixture = load_fixture()
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(
        lambda request: httpx.Response(503, json={"detail": "unavailable"})
    )) as client:
        report = run_evaluation(client, fixture, scenarios=fixture["scenarios"][:1], run_id="failure")
    assert len(report["errors"]) == 1
    assert report["results"] == []
    assert report["summary"]["graph_on"]["expected_fact_recall"] is None


def test_recall_delta_requires_complete_pairs_and_tracks_unknown_pricing():
    def row(question, mode, recall, cost):
        return {"scenario_id": "test", "question_id": question, "mode": mode,
                "score": {"expected_fact_recall": recall, "forbidden_fact_matches": []},
                "meta": {"cost": cost, "latency_ms": 1}}
    summary = summarize([row("paired", "graph_off", 0.25, None),
                         row("paired", "graph_on", 0.75, 0.01),
                         row("incomplete", "graph_on", 0.0, 0.01)])
    assert summary["paired_question_count"] == 1
    assert summary["recall_delta"] == 0.5
    assert summary["graph_off"]["unpriced_answers"] == 1
