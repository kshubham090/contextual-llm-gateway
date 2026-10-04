"""Create a local-only demo environment with random secrets; never overwrite a file."""

import json
import secrets
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    path = root / ".env"
    if path.exists():
        raise SystemExit(".env already exists. Keep its configuration or use a fresh checkout for the demo.")
    token, metrics, postgres, neo4j, redis = [secrets.token_hex(24) for _ in range(5)]
    values = {
        "ENVIRONMENT": "development",
        "GATEWAY_API_KEYS": json.dumps({token: "demo-team"}),
        "METRICS_BEARER_TOKEN": metrics,
        "POSTGRES_PASSWORD": postgres,
        "NEO4J_PASSWORD": neo4j,
        "REDIS_PASSWORD": redis,
        "DATABASE_URL": f"postgresql://gateway:{postgres}@127.0.0.1:5433/gateway",
        "NEO4J_URI": "bolt://127.0.0.1:7687",
        "REDIS_URL": f"redis://:{redis}@127.0.0.1:6379/0",
        "RATE_LIMIT_PER_MINUTE": "300",
    }
    with path.open("x") as output:
        path.chmod(0o600)
        output.write("# Local demo only. Add real provider credentials to use app.main.\n")
        output.write("\n".join(f"{key}='{value}'" for key, value in values.items()) + "\n")
    print("Created .env with random local credentials. The gateway token is the key in GATEWAY_API_KEYS.")
    print("Start dependencies: docker compose up -d postgres neo4j redis")
    print("Start the demo: python scripts/demo_server.py")
    print("Open http://127.0.0.1:8001/inspector. Demo generation is synthetic; no provider key is required.")


if __name__ == "__main__":
    main()
