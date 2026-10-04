# Memory Console walkthrough

Memory Console is an operator workspace served by the gateway at `/inspector`. It has no external assets or third-party analytics. It uses the same bearer authentication and strict `(tenant, user, feature)` boundary as the API. The public HTML shell contains no credentials or memory content.

Use a trusted browser on an HTTPS deployment, or the loopback development demo. The token is held in JavaScript memory, never browser storage or URLs. Disconnect and page reload clear it. A tenant token can select users within its tenant; this interface is not an end-user authorization layer. Browser extensions and a compromised operator device remain outside the gateway's boundary.

## Five-minute local workflow

From a fresh checkout, use Python 3.12 or newer and a running Docker installation with Compose support:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python scripts/start_console.py --open
```

The launcher creates a random-credential `.env` only if the default file is absent, starts the three backing services, waits for them and the gateway to become ready, then opens `http://127.0.0.1:8001/inspector`. It installs no dependencies. Existing `.env` values and exported environment overrides are preserved. Opening `app/static/index.html` directly shows a source document without its API server; use the running HTTP console URL. The gateway root URL also redirects to `/inspector`.

1. Find the generated gateway token in the key of `.env`'s `GATEWAY_API_KEYS`. Enter it and select `support-demo` / `customer-support`.
2. Open **Memory library** and select **Load support example**. Four fictional Cedar connector records are ingested directly without generated seed answers. Loading again adds another set; use a fresh scope for a fresh experiment.
3. Open **Playground** and ask the suggested returning-customer question. **Graph + similar memories** uses vector seeds and graph traversal; **Similar memories only** skips graph traversal; **No memory** supplies no retrieved context. Choose **Complete response** for the local evaluation server, or **Live stream** for a streaming-capable provider. **Remember reply** controls persistence; it does not disable memory reads. Exact answer reuse is separately optional.
4. Follow a source chip. The detail view shows the stored question, answer, source identifiers, status, and revision. These are records of supplied context, not proof of factual correctness or a model's internal reasoning.
5. Correct the escalation rule. This creates a new active record, removes the old live text and derived answers, and invalidates the scope's completion cache. Earlier rendered answers are cleared so the next conversation uses the updated record.
6. Select **Forget** or **Forget this scope** to open a confirmation dialog. Live content is erased, not moved to a recoverable trash. Accounting and revision identifiers remain. Neo4j cleanup follows the durable outbox; backups and provider retention have their own policies.

The demo's generation and embeddings are **synthetic transport fixtures**. Its answer explicitly says it echoes supplied context. This makes the entire workflow runnable without paid provider credentials, but it says nothing about model quality or semantic retrieval accuracy. For real inference, configure your existing `.env` using `.env.example` as a reference and run `python scripts/start_console.py --mode configured --open`. This starts the normal gateway with genuine provider responses. If you copied `.env.example` for a fresh configuration, replace the password placeholders in `DATABASE_URL` and `REDIS_URL` with the matching infrastructure passwords before starting; the host launcher uses these loopback URLs directly. The generated demo configuration already contains matching URLs. A separately configured local model server must already be running; the launcher does not install weights or start it.

## Six connected views

- **Overview:** actual recorded completions for the current user/feature over seven UTC calendar days, daily activity, current memory counts, cache reuse, mean/p95 latency, safe model configuration and dependency health. No requests means no latency measurement. Known generation prices are a subtotal; unpriced calls are separate. Failures before durable accounting are absent, so this is not an HTTP error-rate dashboard.
- **Playground:** real chat through the configured provider, with model routing, retrieval selection, output budget, optional system guidance, four turns of optional in-tab conversation history, complete or streaming delivery, and source/timing inspection. The UI flags token-limited or provider-filtered responses. Remembering and exact-cache reuse are independent controls.
- **Memory graph:** a bounded view of recent active PostgreSQL memories and actual Neo4j relationships. Similarity edges and context-source edges have distinct styles. Zoom, pan, search, keyboard node selection and an accessible node list lead to authoritative memory details. The view shows up to 60 nodes by default (API maximum 80) and 320 edges; truncation and projection outages are explicit.
- **Memory library:** load, search and filter visible records; inspect, add, correct or forget memory using revision guards. Search applies to loaded records, with pagination to load more. Removed content is not exposed by the graph or detail API.
- **Compare retrieval:** run the same question sequentially with no memory, vector retrieval and graph retrieval. Caching and memory writes are disabled; accounting remains. Partial failures and cancelled modes are explicit. Results retain actual model/provider identity, revision, sources, timing, usage and finish reason. Inspect and copy the exported JSON. One comparison is exploratory; it does not establish accuracy, and equal revisions do not prevent unrelated new traffic from adding memory between modes.
- **Connections:** configured generation routes, embedding model/device/batching and request limits, plus a copyable native API request with a credential placeholder. It never embeds the active token in that sample. Model/configuration changes still belong to the server environment.

The authenticated read endpoints are `GET /v1/console/overview` and `GET /v1/console/graph`, both requiring `user_id` and `feature_tag`. Tenant identity comes only from the bearer key. Graph previews are rehydrated from PostgreSQL after reading edges, with revision validation to reject concurrent corrections/deletion. Metadata excludes connection URLs and credentials. Health checks probe stores and worker state, not model generation.

Navigation and read requests are fenced by session and operation sequence. Disconnect clears drafts, details, comparison exports and rendered content. A late request from an old scope cannot overwrite a new workspace. A mutation invalidates pending reads before refreshing, so stale content cannot reappear through a delayed browser response.

## Launcher options and existing installations

| Option | Behavior |
|---|---|
| `--mode demo` | Default. Synthetic replies and embeddings; requires authenticated development configuration. |
| `--mode configured` | Normal gateway using the selected provider/model and embedding settings. Requires an existing configuration file. |
| `--project NAME` | Explicit Compose project; defaults to `contextual-gateway-console`. Reuse your existing project name when appropriate. |
| `--no-services` | Do not invoke Docker; use the stores referenced by your connection settings. |
| `--env-file PATH` | Select a configuration file. Custom files must already exist. Exported environment settings take precedence. |
| `--port NUMBER` | Bind only loopback on this port; defaults to 8001. An occupied port is rejected without stopping its owner. |
| `--startup-timeout SECONDS` | Time allowed for each storage/gateway startup stage; defaults to 120. Increase for slower model initialization. |
| `--open` | Open the ready console in the default browser. No credentials are added to the URL or page. |

For an existing installation with running stores:

```bash
python scripts/start_console.py --mode configured --no-services \
  --env-file .env --port 8001 --open
```

Keep the launcher running while using the console. Ctrl+C stops only the gateway child it started, first allowing graceful shutdown. Storage containers and volumes stay available, including after failed startup. The launcher does not remove volumes or stop unrelated processes. To stop only the default project's backing services later, use `docker compose --project-name contextual-gateway-console --env-file .env --file docker-compose.yml stop postgres neo4j redis`; use the same project and env file you launched with. Do not switch project names expecting existing database volumes to move with them.

If storage ports are already occupied, choose `--no-services` with matching credentials or use the existing Compose project. Configured mode also requires correct host-accessible connection URLs: Compose service names such as `postgres` are for containers, while the host launcher normally uses loopback. Startup errors do not expose parsed configuration values, and the launcher never prints a bearer token.

## Streaming and concurrent edits

Text deltas are provisional. Only the final event confirms durable completion. Stopping a stream cancels active upstream work where possible; a response already committed immediately before stopping may still appear in memory. Native model kernels and external provider billing are not necessarily cancelled instantly.

If a memory mutation races with an answer, the answer cannot persist content from the old scope revision. The client receives a conflict or a terminal stream error. Refresh and make a new request against the current memory; the SDK does not silently retry a charged operation.

List pagination is tied to a scope revision and a snapshot. A concurrent correction or deletion invalidates a cursor with HTTP 409. Reload the timeline to get a new cursor. Browser behavior tests cover navigation, delayed scope/detail/graph responses, mutation fences, partial comparisons, cancellation, genuine complete-response delivery, unavailable usage, empty latency, credential-free samples and literal text rendering. Real browser verification remains a separate check.
