"""Server-side synthetic support walkthrough; one generation request may incur charges."""

import argparse
import asyncio
import os
import sys
from uuid import uuid4

from lowq_client import (
    AsyncGateway,
    GatewayError,
    GatewayProtocolError,
    GatewayStreamError,
    GatewayTransportError,
)


async def walkthrough(*, cleanup: bool) -> None:
    api_key = os.environ.get("GATEWAY_API_KEY")
    if not api_key:
        raise SystemExit("Set GATEWAY_API_KEY in this trusted server/terminal environment first.")
    # Fresh fictional identities keep this demo separate from all customer memory.
    scope = {"user_id": f"support-demo-{uuid4().hex}", "feature_tag": "support-demo"}
    print(f"Synthetic user: {scope['user_id']}")
    async with AsyncGateway(
        api_key=api_key, base_url=os.environ.get("GATEWAY_BASE_URL", "http://127.0.0.1:8000"),
    ) as gateway:
        page = await gateway.list_memories(**scope)
        seeded = await gateway.create_memory(
            **scope,
            prompt="Earlier support case: fictional Cedar connector CDR-409 after a workspace migration.",
            response="The initial advice was to refresh the connector cursor, then resume sync.",
            expected_scope_revision=page["scope_revision"],
        )
        corrected = await gateway.correct_memory(
            seeded["memory"]["id"], **scope,
            prompt="Reviewed support rule for fictional Cedar CDR-409 after a workspace migration.",
            response=(
                "Refresh the connector cursor, run a dry-run reconciliation, and verify workspace "
                "ownership before resuming sync. If CDR-409 persists, escalate with reconciliation "
                "output and the workspace migration ID. Do not delete the workspace."
            ),
            expected_revision=seeded["memory"]["revision"],
        )
        print(f"Corrected source: {corrected['memory']['id']}")
        print("\nAssistant (provisional until completed):")
        async with gateway.stream_chat({
            **scope,
            "prompt": "My Cedar CDR-409 connector issue is back. What should I do before resuming sync?",
            "use_cache": False,
            "store": False,
        }) as events:
            async for event in events:
                if event["type"] == "delta":
                    print(event["delta"], end="", flush=True)
                else:
                    print("\n\nCompleted. Sources:", event["response"]["meta"].get("context_used", []))
        page = await gateway.list_memories(**scope)
        print("Memory status:", [(item["id"], item["status"]) for item in page["items"]])
        if cleanup:
            removed = await gateway.delete_scope(**scope, expected_scope_revision=page["scope_revision"])
            print("Deleted demo scope. Graph cleanup:", removed["graph_write"])
        else:
            print("Demo memory remains in this synthetic scope until expiry or explicit deletion.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cleanup", action="store_true", help="Delete this fresh synthetic scope after the demo",
    )
    args = parser.parse_args()
    try:
        asyncio.run(walkthrough(cleanup=args.cleanup))
    except (GatewayError, GatewayProtocolError, GatewayStreamError, GatewayTransportError) as exc:
        print(f"\nDemo failed: {exc}. No automatic retry was attempted.", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
