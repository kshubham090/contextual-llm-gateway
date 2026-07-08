"""The pitch in one screenshot: the same fresh question, answered twice —
once with the memory graph bypassed, once with graph context injected.

Run scripts/seed_demo.py first so the graph has something to remember.

Usage:
    python scripts/demo_compare.py [--base-url http://localhost:8000] [--prompt "..."]
"""
import argparse
import textwrap

import httpx

# A question the seed data never asked directly, but which the graph's
# accumulated domain knowledge (bake times, flight numbers, config reverts,
# error-budget gates, canary aborts...) should visibly ground.
DEFAULT_PROMPT = (
    "I'm the new on-call for a service deployed through Orbit. A deploy just "
    "went bad in production. Walk me through exactly what I should do."
)


def ask(client: httpx.Client, prompt: str, use_graph: bool) -> dict:
    resp = client.post(
        "/v1/chat",
        json={
            "prompt": prompt,
            "user_id": "demo-user",
            "feature_tag": "demo",
            "max_tokens": 700,
            "use_graph": use_graph,
            # Keep the comparison clean and repeatable: don't serve either
            # answer from cache, and don't write demo calls into the graph.
            "use_cache": False,
            "store": False,
        },
    )
    resp.raise_for_status()
    return resp.json()


def block(title: str, body: str, meta: dict) -> str:
    header = (
        f"=== {title} ===\n"
        f"model={meta.get('model')}  context_calls={len(meta.get('context_used', []))}  "
        f"cost=${meta.get('cost', 0):.4f}  latency={meta.get('latency_ms')}ms\n"
    )
    wrapped = "\n".join(
        textwrap.fill(line, width=100) if line else "" for line in body.splitlines()
    )
    return header + "-" * 100 + "\n" + wrapped + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    args = parser.parse_args()

    client = httpx.Client(base_url=args.base_url, timeout=180.0)

    print(f"Question: {args.prompt}\n")

    without = ask(client, args.prompt, use_graph=False)
    with_graph = ask(client, args.prompt, use_graph=True)

    print(block("WITHOUT graph memory (plain gateway)", without["response"], without["meta"]))
    print()
    print(block("WITH graph memory (context injected)", with_graph["response"], with_graph["meta"]))

    n_ctx = len(with_graph["meta"].get("context_used", []))
    print(
        f"\nThe second answer was grounded by {n_ctx} related past calls pulled "
        f"from the Neo4j memory graph — same question, same model, no extra "
        f"prompt engineering by the caller."
    )


if __name__ == "__main__":
    main()
