# Synthetic scenario packs

`scenarios.json` contains six fictional domains, each with four seed memories and two held-out questions. Expected facts use accepted phrase aliases; forbidden facts capture concrete contradictions or cross-scope markers. These are fixture assertions, not expert-validated domain policies.

```bash
python scripts/evaluate.py validate
python scripts/evaluate.py run --scenario orbit-incident --output artifacts/orbit.json
```

Validation is offline. Live evaluation needs a running gateway and `GATEWAY_API_KEY` and incurs provider charges. Read the [evaluation protocol](../docs/evaluation.md) before interpreting scores; a lexical match is not proof of correctness, and a forbidden phrase can appear under negation.
