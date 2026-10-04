# Memory Studio walkthrough

Memory Studio is an operator tool served by the gateway at `/inspector`. It has no external assets or third-party analytics. It uses the same bearer authentication and strict `(tenant, user, feature)` boundary as the API. The public HTML shell contains no credentials or memory content.

Use a trusted browser on an HTTPS deployment, or the loopback development demo. The token is held in JavaScript memory, never browser storage or URLs. Disconnect and page reload clear it. A tenant token can select users within its tenant; this interface is not an end-user authorization layer. Browser extensions and a compromised operator device remain outside the gateway's boundary.

## Five-minute local workflow

From a fresh checkout, install `requirements-dev.txt`, run `python scripts/setup_demo.py`, and start `docker compose up -d postgres neo4j redis`. Then run `python scripts/demo_server.py` and open `http://127.0.0.1:8001/inspector`.

1. Find the generated gateway token in the key of `.env`'s `GATEWAY_API_KEYS`. Enter it and select `support-demo` / `customer-support`.
2. Select **Load support example**. Four fictional Cedar connector records are ingested directly without generated seed answers. Loading again adds another set; use a fresh scope for a fresh experiment.
3. Ask the suggested returning-customer question. **Connected memory** uses vector seeds and graph traversal; **Similar memories** skips graph traversal; **No memory** supplies no retrieved context. **Remember reply** controls persistence; it does not disable memory reads. Exact answer reuse is separately optional.
4. Follow a source chip. The detail view shows the stored question, answer, source identifiers, status, and revision. These are records of supplied context, not proof of factual correctness or a model's internal reasoning.
5. Correct the escalation rule. This creates a new active record, removes the old live text and derived answers, and invalidates the scope's completion cache. Earlier rendered answers are cleared so the next conversation uses the updated record.
6. Select **Forget** or **Forget this scope** to open a confirmation dialog. Live content is erased, not moved to a recoverable trash. Accounting and revision identifiers remain. Neo4j cleanup follows the durable outbox; backups and provider retention have their own policies.

The demo's generation and default embeddings are **synthetic transport fixtures**. Its answer explicitly says it echoes supplied context. This makes the entire workflow runnable without API credentials, but it says nothing about model quality or semantic retrieval accuracy. For real inference, configure your existing `.env` using `.env.example` as a reference and launch `app.main:app` or the regular Docker app. The inspector then uses genuine provider streaming.

## Streaming and concurrent edits

Text deltas are provisional. Only the final event confirms durable completion. Stopping a stream cancels active upstream work where possible; a response already committed immediately before stopping may still appear in memory. Native model kernels and external provider billing are not necessarily cancelled instantly.

If a memory mutation races with an answer, the answer cannot persist content from the old scope revision. The client receives a conflict or a terminal stream error. Refresh and make a new request against the current memory; the SDK does not silently retry a charged operation.

List pagination is tied to a scope revision and a snapshot. A concurrent correction or deletion invalidates a cursor with HTTP 409. Reload the timeline to get a new cursor. The inspector's browser tests cover clearing drafts across scopes, durable final events followed by refresh failures, incomplete streams, and text-only rendering.
