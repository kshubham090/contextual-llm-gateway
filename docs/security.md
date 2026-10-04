# Security and data boundaries

## Identity

`GATEWAY_API_KEYS` is a JSON mapping from random bearer tokens to tenant IDs. Tokens must contain at least 32 characters; use cryptographically generated values. A tenant is never accepted from request JSON. Rotate credentials by temporarily accepting both old and new tokens for the same tenant, updating clients, then removing the old token and restarting replicas.

The authenticated caller is a **trusted application**, not an end user. It can choose any `user_id` in its tenant and can query tenant-wide usage when omitting filters. Bind real user identity to `user_id` in your application; do not expose the tenant token to browsers or mobile apps. Application-enforced scope isolation is not database row-level security and does not protect against a compromised gateway process or a privileged database operator.

All cache lookups, vector searches, graph neighborhoods, and graph counts use tenant scope. Retrieval adds exact user and feature boundaries. Usage reports remain tenant-scoped. `/metrics` contains global operating information and uses a separate bearer token. Health probes do not require tenant credentials.

## Content handling

With `store: true`, prompts, generated answers, embeddings, and relationship payloads may be retained in PostgreSQL and Neo4j. Memory can contain personal, confidential, incorrect, or malicious material. There is no automatic PII detector, redactor, or policy classifier.

With `store: false`, the gateway does not retain prompt text, answer text, embeddings, or graph events in its memory stores or embedding cache. Accounting identifiers, token counts, model information, cost estimates, and durations remain. The request can still read authorized prior memory. Providers still process the prompt and supplied context; their retention terms apply independently. This is a gateway memory control, not a guarantee of zero processing or zero metadata retention.

Expired memory is excluded from retrieval. Physical erasure requires coordinated PostgreSQL and Neo4j maintenance, pending-event handling, and backup policies. There is no complete per-person deletion API. Legacy pre-authentication memory is quarantined during migration; quarantine is not deletion.

## Prompt and cache safety

Retrieved exchanges are untrusted material. Context delimiting, escaping, count limits, and total size budgets reduce accidental instruction blending. They cannot prevent all prompt injection or model hallucination. Do not use model answers as authorization decisions or execute returned commands without a separate, appropriate control layer.

Exact completion caching is the default. Semantic caching is opt-in because small changes in dates, negation, quantities, or customer state can make near-identical requests require different answers. Both modes remain TTL- and scope-bound, and generation configuration participates in eligibility. Disable cache for evaluation and for workflows whose answer changes independently of the prompt.

## Deployment responsibilities

Terminate TLS at a trusted ingress; restrict access to PostgreSQL, Neo4j, and Redis to the private service network. Use managed secrets, encryption at rest, limited service identities, reviewed backup access, and audited images. The supplied Compose file binds ports to loopback and is for local development. Do not publish its service ports on the internet.

Bounded input and concurrency reduce resource exhaustion; they are not a substitute for upstream request filtering or a cost budget. Per-process circuits and caches reset on restart. Tenant bearer keys are long-lived shared secrets, not OAuth/OIDC, fine-grained role grants, or end-user sessions. Perform a deployment-specific threat review before handling regulated or high-impact data.

Report suspected vulnerabilities through the repository's private security reporting channel when enabled; avoid opening a public issue containing credentials, user data, or working exploit payloads.
