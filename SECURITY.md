# Security policy

This repository is pre-1.0 software. Security fixes are developed against the
current main branch; there is no commitment to backport fixes to older revisions.
Check the changelog and deployment guidance before upgrading. The application
must be assessed for the data and environment in which it is deployed.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/kshubham090/contextual-llm-gateway/security/advisories/new)
when enabled. Include the affected revision, a minimal synthetic reproduction,
expected versus observed behavior, and the relevant trust boundary. Do not send
live credentials, personal data, or production memory exports.

If private reporting is unavailable, open a minimal issue asking the maintainer
for a private reporting channel, without vulnerability details. Do not post a
working exploit or sensitive material publicly. Response times are best effort;
this project has no paid security-response SLA.

## Boundaries to understand

A tenant bearer key belongs to a trusted application and can choose identities
within that tenant. End-user authentication and authorization are application
responsibilities. Never put tenant keys in browser/mobile bundles. Strict
tenant/user/feature filters are application isolation, not protection against
privileged database operators or a compromised gateway process.

Memory may contain malicious, stale, or incorrect text. Context delimiters and
size limits do not eliminate prompt injection. The gateway does not authorize
business actions or execute tools. A source identifier establishes provenance,
not answer correctness.

`store: false` prevents new content retention by gateway memory; it does not stop
authorized memory reads, content-free accounting, or provider processing.
Deletion/invalidation of live memory and graph projection, backup expiry, and
external provider retention are different operations. No API alone certifies
complete personal-data erasure or compliance.

The supplied Compose stack is a local development setup. TLS, private service
networks, secrets rotation, monitoring, backup access, and rollout/rollback
procedures belong to deployment configuration. Read
[security boundaries](docs/security.md) and [operations](docs/operations.md).
