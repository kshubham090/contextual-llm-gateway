/** Run on a trusted server/terminal. One generation request may incur provider charges. */
import { randomUUID } from "node:crypto";
import { GatewayClient } from "../sdk/typescript/dist/index.js";

const args = process.argv.slice(2);
if (args.includes("--help")) {
  console.log("node examples/support-typescript.mjs [--cleanup]\nCreates a fresh synthetic support scope; --cleanup deletes it after the demo.");
  process.exit(0);
}
if (args.some((arg) => arg !== "--cleanup")) {
  console.error("Unknown option. Use --help.");
  process.exit(1);
}
if (!process.env.GATEWAY_API_KEY) {
  console.error("Set GATEWAY_API_KEY in this trusted server/terminal environment first.");
  process.exit(1);
}

const gateway = new GatewayClient({
  apiKey: process.env.GATEWAY_API_KEY,
  baseUrl: process.env.GATEWAY_BASE_URL ?? "http://127.0.0.1:8000",
});
// A fresh fictional scope cannot read or modify existing customers' memory.
const scope = { user_id: `support-demo-${randomUUID()}`, feature_tag: "support-demo" };
console.log("Synthetic user:", scope.user_id);

try {
  let page = await gateway.listMemories(scope);
  const seeded = await gateway.createMemory({
    ...scope,
    prompt: "Earlier support case: fictional Cedar connector CDR-409 after a workspace migration.",
    response: "The initial advice was to refresh the connector cursor, then resume sync.",
    expected_scope_revision: page.scope_revision,
  });
  const corrected = await gateway.correctMemory(seeded.memory.id, {
    ...scope,
    prompt: "Reviewed support rule for fictional Cedar CDR-409 after a workspace migration.",
    response: "Refresh the connector cursor, run a dry-run reconciliation, and verify workspace ownership before resuming sync. If CDR-409 persists, escalate with reconciliation output and the workspace migration ID. Do not delete the workspace.",
    expected_revision: seeded.memory.revision,
  });
  console.log("Corrected source:", corrected.memory.id);
  console.log("\nAssistant (provisional until completed):");
  for await (const event of gateway.streamChat({
    ...scope,
    prompt: "My Cedar CDR-409 connector issue is back. What should I do before resuming sync?",
    use_cache: false,
    store: false,
  })) {
    if (event.type === "delta") process.stdout.write(event.delta);
    else console.log("\n\nCompleted. Sources:", event.response.meta.context_used);
  }
  page = await gateway.listMemories(scope);
  console.log("Memory status:", page.items.map(({ id, status }) => ({ id, status })));
  if (args.includes("--cleanup")) {
    const removed = await gateway.deleteScope({ ...scope, expected_scope_revision: page.scope_revision });
    console.log("Deleted demo scope. Graph cleanup:", removed.graph_write);
  } else {
    console.log("Demo memory remains in this synthetic scope until expiry or explicit deletion.");
  }
} catch (error) {
  console.error(`\nDemo failed: ${error.message}. No automatic retry was attempted.`);
  process.exitCode = 1;
}
