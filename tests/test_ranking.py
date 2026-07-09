import time

from app.config import settings
from app.pipeline import build_context_blob, rank_candidates

NOW = time.time()
DAY = 86400


def _call(id_, feature="deploys", age_days=0.0):
    return {
        "id": id_,
        "prompt": f"prompt {id_}",
        "response": f"response {id_}",
        "feature_tag": feature,
        "created_epoch": NOW - age_days * DAY,
    }


def test_direct_seed_outranks_hop_discovered_node():
    candidates = [_call("hop"), _call("seed")]
    ranked = rank_candidates(candidates, {"seed": 0.92}, "deploys", now=NOW)
    assert [c["id"] for c in ranked] == ["seed", "hop"]


def test_recent_call_outranks_old_call():
    candidates = [_call("old", age_days=30), _call("new", age_days=0)]
    ranked = rank_candidates(candidates, {}, "deploys", now=NOW)
    assert ranked[0]["id"] == "new"


def test_same_feature_outranks_other_feature():
    candidates = [_call("other", feature="billing"), _call("mine", feature="deploys")]
    ranked = rank_candidates(candidates, {}, "deploys", now=NOW)
    assert ranked[0]["id"] == "mine"


def test_recency_halves_at_half_life():
    half_life = settings.recency_half_life_days
    fresh = rank_candidates([_call("a", age_days=0)], {}, "x", now=NOW)[0]["rank_score"]
    aged = rank_candidates([_call("a", age_days=half_life)], {}, "x", now=NOW)[0]["rank_score"]
    # only the recency component (weight w_rec) differs, halved
    expected_drop = settings.rank_weight_recency * 0.5
    assert fresh - aged == abs(fresh - aged)  # fresh scores higher
    assert abs((fresh - aged) - expected_drop) < 1e-6


def test_every_candidate_gets_a_rank_score():
    ranked = rank_candidates([_call("a"), _call("b")], {"a": 0.8}, "deploys", now=NOW)
    assert all("rank_score" in c for c in ranked)


def test_context_blob_contains_trimmed_exchanges():
    calls = [
        {"id": "1", "prompt": "How do rollbacks work?", "response": "Via config revert.",
         "feature_tag": "deploys"},
        {"id": "2", "prompt": "x" * 5000, "response": "y" * 5000, "feature_tag": "config"},
    ]
    blob = build_context_blob(calls)
    assert "<related_past_calls>" in blob
    assert "How do rollbacks work?" in blob
    assert "Via config revert." in blob
    assert "(feature: deploys)" in blob
    # long entries are trimmed, not injected wholesale
    assert "x" * (settings.context_snippet_chars + 1) not in blob
    assert "…" in blob
