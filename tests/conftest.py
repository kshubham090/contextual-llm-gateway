# Dummy credentials so app.config imports without a real .env — no test
# ever talks to a live service.
import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault("VOYAGE_API_KEY", "test-key")
